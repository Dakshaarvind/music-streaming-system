#!/usr/bin/env python3
"""
Automated end-to-end demo for Milestone 4 (one terminal).

Spawns 5 node processes, then walks through:
  1. Bully leader election on startup
  2. Concurrent edits WITHOUT mutual exclusion  -> lost updates (the problem)
  3. Concurrent edits WITH centralized mutex     -> all edits kept, serialized
  4. Coordinator CRASH while the lock is held and others are queued
     -> failure detected, re-election, lock state recovered, queue drained
  5. Old leader restarts -> Bully: highest id takes leadership back
  6. Merged, Lamport-ordered event log + automatic verification

    python3 -m milestone4.demo
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

from . import client as CL
from . import common as C
from . import merge_logs as ML

NODES = C.parse_nodes(None)
PROCS = {}


def banner(title: str, *lines: str) -> None:
    print("\n" + "=" * 100, flush=True)
    print(f" {title}")
    for ln in lines:
        print(f"   --> {ln}")
    print("=" * 100, flush=True)


def start_node(nid: int) -> None:
    PROCS[nid] = subprocess.Popen([sys.executable, "-m", "milestone4.node", "--id", str(nid)],
                                  cwd=C.ROOT)


def wait_for(pred, timeout: float, step: float = 0.2) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(step)
    return False


def leader_is(nid: int) -> bool:
    st = CL.status(NODES)
    live = [s for s in st.values() if s]
    return bool(live) and all(s["leader"] == nid and not s["electing"] for s in live)


def tracks(unsafe=False) -> int:
    from . import shared_playlist as SP
    return len(SP.read(SP.path_for(C.DEFAULT_DATA_DIR, unsafe))["tracks"])


def idle() -> bool:
    return all(not s["want"] and not s["holding"] for s in CL.status(NODES).values() if s)


def pause(msg: str = "") -> None:
    if os.environ.get("M4_STEP"):
        input(f"\n[press Enter to continue{': ' + msg if msg else ''}] ")
    else:
        time.sleep(1.0)


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)
    CL.reset(C.DEFAULT_DATA_DIR, C.DEFAULT_LOG_DIR)
    try:
        banner("PHASE 1: STARTUP + BULLY LEADER ELECTION",
               "5 node processes, each with its own Lamport clock (ports 9101-9105)",
               "Every message carries 'msg ts'; receivers apply clock = max(local, msg ts) + 1",
               "Expected: highest id (N5) wins the election")
        for nid in sorted(NODES):
            start_node(nid)
            time.sleep(0.4)
        wait_for(lambda: leader_is(5), 15)
        print()
        CL.print_status(NODES)
        pause("unsafe writes")

        banner("PHASE 2: SHARED RESOURCE WITHOUT COORDINATION (unsafe mode)",
               "All 5 nodes add a track to the playlist file at the same instant, no lock",
               "Expected: CONFLICT warnings and LOST UPDATES (fewer than 5 tracks survive)")
        CL.burst(NODES, unsafe=True)
        wait_for(lambda: idle() and tracks(True) > 0, 10)
        time.sleep(1.5)
        print()
        CL.print_playlist(C.DEFAULT_DATA_DIR, unsafe=True)
        pause("protected writes")

        banner("PHASE 3: CENTRALIZED MUTUAL EXCLUSION (coordinator = elected leader)",
               "Same 5 concurrent edits, but each node must REQUEST -> GRANT -> edit -> RELEASE",
               "Coordinator queues requests ordered by Lamport req_ts (ties -> node id)",
               "Expected: exactly one node in CS_ENTER..CS_EXIT at a time, all 5 tracks kept")
        CL.burst(NODES)
        wait_for(lambda: tracks() >= 5 and idle(), 20)
        print()
        CL.print_playlist(C.DEFAULT_DATA_DIR)
        pause("coordinator failure")

        banner("PHASE 4: COORDINATOR FAILURE WHILE THE LOCK IS HELD",
               "N1-N4 request the lock together (each holds it 3.5s); once a holder is granted",
               "and the others are queued, coordinator N5 is CRASHED (no goodbye messages)",
               "Expected: heartbeat timeout -> Bully election -> N4 leads -> recovers holder + queue")
        CL.burst(NODES, hold=3.5, only=[1, 2, 3, 4])

        def queued() -> bool:
            s = CL.status(NODES).get(5) or {}
            c = s.get("coordinator") or {}
            return bool(c.get("holder")) and len(c.get("queue", [])) >= 2
        wait_for(queued, 5, 0.05)
        print()
        CL.print_status(NODES)
        print("\n>>> CRASHING COORDINATOR N5 NOW <<<\n", flush=True)
        CL.call(NODES, 5, "CLIENT_CRASH")
        PROCS[5].wait(5)
        wait_for(lambda: leader_is(4), 15)
        print()
        CL.print_status(NODES)
        wait_for(lambda: tracks() >= 9 and idle(), 30)
        print()
        CL.print_playlist(C.DEFAULT_DATA_DIR)
        pause("old leader returns")

        banner("PHASE 5: OLD COORDINATOR RESTARTS (Bully: highest id takes over again)",
               "N5 restarts, recovers its Lamport clock from its log, learns the current term,",
               "holds an election and bullies its way back; then N1 edits through the new coordinator")
        start_node(5)
        wait_for(lambda: leader_is(5), 15)
        time.sleep(0.5)
        print()
        CL.print_status(NODES)
        CL.add(NODES, 1, title="Encore (after N5 returned)")
        wait_for(lambda: tracks() >= 10 and idle(), 10)
        time.sleep(0.5)
    finally:
        for p in PROCS.values():
            if p.poll() is None:
                p.terminate()
        for p in PROCS.values():
            try:
                p.wait(3)
            except subprocess.TimeoutExpired:
                p.kill()
        time.sleep(0.3)

    banner("PHASE 6: EVENT ORDERING - all node logs merged in (Lamport, node id) order",
           "Key coordination events only; full history: python3 -m milestone4.merge_logs")
    events = ML.load(C.DEFAULT_LOG_DIR)
    ML.show(events, key_only=True)
    print()
    CL.print_playlist(C.DEFAULT_DATA_DIR)
    ok = ML.verify(events, C.DEFAULT_DATA_DIR)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
