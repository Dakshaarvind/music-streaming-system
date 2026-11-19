"""
Milestone 4 tests: Lamport clock rules (unit) plus a live 3-node cluster
(separate OS processes) for election, mutual exclusion, coordinator failure
and lock-holder failure.

    python -m pytest tests/test_milestone4.py -v
"""
import os
import subprocess
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from milestone4 import client as CL            # noqa: E402
from milestone4 import common as C             # noqa: E402
from milestone4 import merge_logs as ML        # noqa: E402
from milestone4 import shared_playlist as SP   # noqa: E402


# ---------------------------------------------------------------- unit tests
def test_lamport_local_and_send_increment():
    c = C.LamportClock()
    assert c.tick() == 1
    assert c.send() == 2


def test_lamport_receive_takes_max_plus_one():
    c = C.LamportClock(5)
    assert c.recv(10) == (5, 11)      # remote clock ahead
    assert c.recv(3) == (11, 12)      # remote clock behind: still advances


def test_fencing_token_ordering():
    assert SP.token_ok([2, 1], [1, 9])        # newer term wins
    assert SP.token_ok([2, 2], [2, 1])
    assert not SP.token_ok([1, 9], [2, 1])    # stale holder from an old term
    assert not SP.token_ok(None, [0, 0])


# ------------------------------------------------------- live cluster tests
NODES = {1: 9211, 2: 9212, 3: 9213}


def wait_for(pred, timeout, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(step)
    return False


def leader_everywhere(expected):
    live = [s for s in CL.status(NODES).values() if s]
    return bool(live) and all(s["leader"] == expected and not s["electing"] for s in live)


def idle():
    return all(not s["want"] and not s["holding"] for s in CL.status(NODES).values() if s)


@pytest.fixture
def cluster(tmp_path):
    data_dir, log_dir = str(tmp_path / "data"), str(tmp_path / "logs")
    SP.reset(data_dir)
    procs = {}

    def start(nid):
        procs[nid] = subprocess.Popen(
            [sys.executable, "-m", "milestone4.node", "--id", str(nid), "--nodes", C.nodes_to_spec(NODES),
             "--data-dir", data_dir, "--log-dir", log_dir, "--no-color", "--cs-time", "0.3"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    for n in NODES:
        start(n)
    yield {"data": data_dir, "logs": log_dir, "start": start, "procs": procs}
    for p in procs.values():
        if p.poll() is None:
            p.terminate()
    for p in procs.values():
        try:
            p.wait(3)
        except subprocess.TimeoutExpired:
            p.kill()


def tracks(data_dir):
    return SP.read(SP.path_for(data_dir, False))["tracks"]


def test_bully_elects_highest_id(cluster):
    assert wait_for(lambda: leader_everywhere(3), 10)


def test_mutex_serializes_concurrent_writes(cluster):
    assert wait_for(lambda: leader_everywhere(3), 10)
    CL.burst(NODES)
    assert wait_for(lambda: len(tracks(cluster["data"])) == 3 and idle(), 15)
    events = ML.load(cluster["logs"])
    assert ML.check_mutex(events, "safe") == []
    assert ML.check_causality(events) == []
    assert not [e for e in events if e["event"] == "CONFLICT"]


def test_unsafe_mode_loses_updates(cluster):
    assert wait_for(lambda: leader_everywhere(3), 10)
    CL.burst(NODES, unsafe=True)
    time.sleep(2)
    assert len(SP.read(SP.path_for(cluster["data"], True))["tracks"]) < 3


def test_coordinator_crash_reelection_and_recovery(cluster):
    assert wait_for(lambda: leader_everywhere(3), 10)
    CL.burst(NODES, hold=2.5, only=[1, 2])
    assert wait_for(lambda: (CL.status(NODES)[3] or {}).get("coordinator", {}).get("holder"), 5, 0.05)
    CL.call(NODES, 3, "CLIENT_CRASH")
    assert wait_for(lambda: leader_everywhere(2), 10)
    assert wait_for(lambda: len(tracks(cluster["data"])) == 2 and idle(), 20)
    events = ML.load(cluster["logs"])
    assert ML.check_mutex(events, "safe") == []
    assert any(e["event"] == "RECOVERED" for e in events)          # holder state rebuilt

    cluster["start"](3)                                            # old leader returns
    assert wait_for(lambda: leader_everywhere(3), 10)
    CL.add(NODES, 1)
    assert wait_for(lambda: len(tracks(cluster["data"])) == 3 and idle(), 10)
    toks = [tuple(t["token"]) for t in tracks(cluster["data"])]
    assert toks == sorted(toks)                                    # fencing tokens monotonic


def test_lock_holder_crash_is_revoked(cluster):
    assert wait_for(lambda: leader_everywhere(3), 10)
    CL.add(NODES, 1, hold=30)                                      # N1 grabs the lock and sits on it
    assert wait_for(lambda: (CL.status(NODES)[1] or {}).get("holding"), 5, 0.05)
    CL.add(NODES, 2)                                               # N2 queues behind it
    time.sleep(0.5)
    CL.call(NODES, 1, "CLIENT_CRASH")
    assert wait_for(lambda: len(tracks(cluster["data"])) == 1 and idle(), 10)
    assert tracks(cluster["data"])[0]["added_by"] == "N2"
    assert any(e["event"] == "HOLDER_FAILED" for e in ML.load(cluster["logs"]))
