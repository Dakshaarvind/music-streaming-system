#!/usr/bin/env python3
"""
Event ordering: merge every node's log into ONE totally ordered history.

Order = (Lamport timestamp, node id). Lamport guarantees that if event a
happened-before event b then L(a) < L(b); breaking ties with the node id
turns the partial order into a total order that every node would agree on.

    python3 -m milestone4.merge_logs                # full ordered history
    python3 -m milestone4.merge_logs --key          # only election / lock / CS events
    python3 -m milestone4.merge_logs --verify       # + automatic correctness checks
"""
from __future__ import annotations

import argparse
import glob
import json
import os
from datetime import datetime
from typing import List

from . import common as C
from . import shared_playlist as SP

KEY_EVENTS = {
    "NODE_START", "ELECTION_START", "BULLY", "SHUTDOWN", "LEADER_ELECTED", "LEADER_ACCEPTED", "LEADER_FAILED", "CRASH",
    "RECOVERY", "RECOVERED", "RECOVERY_DONE", "CS_REQUEST", "LOCK_QUEUED", "LOCK_GRANTED",
    "LOCK_RELEASED", "CS_ENTER", "CS_EXIT", "CONFLICT", "HOLDER_FAILED", "FENCED", "UNSAFE_MODE",
}


def load(log_dir: str) -> List[dict]:
    events = []
    for path in glob.glob(os.path.join(log_dir, "node-*.jsonl")):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass                       # torn last line from a crash
    events.sort(key=lambda e: (e["lamport"], e["node"]))
    return events


def show(events: List[dict], key_only: bool) -> None:
    print(f"{'#':>4}  {'L':>5}  {'node':<4}  {'wall clock':<12}  {'event':<15}  detail")
    print("-" * 110)
    for i, e in enumerate(events, 1):
        if key_only and e["event"] not in KEY_EVENTS:
            continue
        print(f"{i:>4}  {e['lamport']:>5}  N{e['node']:<3}  {e['wall'][11:23]:<12}  {e['event']:<15}  {e['detail']}")


def _wall(e: dict) -> float:
    return datetime.strptime(e["wall"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()


def check_causality(events: List[dict]) -> List[str]:
    """Every RECV must have a larger Lamport time than its matching SEND."""
    sends = {e["msg_id"]: e for e in events if e["event"] == "SEND" and e.get("msg_id")}
    bad, pairs = [], 0
    for e in events:
        if e["event"] == "RECV" and e.get("msg_id") in sends:
            pairs += 1
            s = sends[e["msg_id"]]
            if not s["lamport"] < e["lamport"]:
                bad.append(f"{e['msg_id']}: send L={s['lamport']} recv L={e['lamport']}")
    print(f"[causality]   {pairs - len(bad)}/{pairs} send->receive pairs satisfy L(send) < L(receive)")
    return bad


def check_mutex(events: List[dict], mode: str) -> List[str]:
    """Look for overlapping critical sections, using real (wall-clock) time on
    this machine and, separately, Lamport order."""
    cs = [e for e in events if e["event"] in ("CS_ENTER", "CS_EXIT", "CRASH") and e.get("mode", mode) == mode]
    # wall-clock intervals per node
    intervals, open_ = [], {}
    for e in sorted(cs, key=_wall):
        if e["event"] == "CS_ENTER":
            open_[e["node"]] = e
        elif e["node"] in open_:
            s = open_.pop(e["node"])
            intervals.append((_wall(s), _wall(e), e["node"]))
    for n, s in open_.items():                            # crashed inside the CS
        intervals.append((_wall(s), float("inf"), n))
    intervals.sort()
    overlaps = []
    for (a0, a1, an), (b0, b1, bn) in zip(intervals, intervals[1:]):
        if b0 < a1:
            overlaps.append(f"N{an} and N{bn} were inside the critical section at the same time")
    # Lamport-order check: ENTER/EXIT must strictly alternate
    inside, lamport_bad = None, 0
    for e in [x for x in events if x["event"] in ("CS_ENTER", "CS_EXIT") and x.get("mode") == mode]:
        if e["event"] == "CS_ENTER":
            if inside is not None:
                lamport_bad += 1
            inside = e["node"]
        elif inside == e["node"]:
            inside = None
    label = "[mutex:safe] " if mode == "safe" else "[mutex:unsafe]"
    n_cs = sum(1 for e in cs if e["event"] == "CS_ENTER")
    print(f"{label} {n_cs} critical sections, {len(overlaps)} real-time overlaps, "
          f"{lamport_bad} Lamport-order interleavings")
    return overlaps


def check_playlist(data_dir: str, events: List[dict]) -> List[str]:
    data = SP.read(SP.path_for(data_dir, False))
    exits = sum(1 for e in events if e["event"] == "CS_EXIT" and e.get("mode") == "safe")
    tracks = len(data["tracks"])
    tokens = [tuple(t["token"]) for t in data["tracks"]]
    problems = []
    if tracks != exits:
        problems.append(f"{exits} safe writes completed but playlist holds {tracks} tracks (lost update)")
    if tokens != sorted(tokens):
        problems.append("fencing tokens in playlist are not increasing")
    print(f"[playlist]    {exits} committed writes, {tracks} tracks in file, version={data['version']}, "
          f"tokens increasing: {tokens == sorted(tokens)}")
    unsafe = SP.read(SP.path_for(data_dir, True))
    u_exits = sum(1 for e in events if e["event"] == "CS_EXIT" and e.get("mode") == "unsafe")
    if u_exits:
        print(f"[unsafe file] {u_exits} writes completed but only {len(unsafe['tracks'])} tracks survived "
              f"-> {u_exits - len(unsafe['tracks'])} LOST UPDATES without coordination")
    return problems


def verify(events: List[dict], data_dir: str) -> bool:
    print("\n=== VERIFICATION ===")
    bad = check_causality(events)
    bad += check_mutex(events, "safe")
    check_mutex(events, "unsafe")                      # expected to show the problem
    bad += check_playlist(data_dir, events)
    conflicts = [e for e in events if e["event"] == "CONFLICT" and e.get("mode") == "safe"]
    bad += [f"safe-mode CONFLICT: {e['detail']}" for e in conflicts]
    print("RESULT:", "PASS - no conflicting operations in protected mode" if not bad else "FAIL")
    for b in bad:
        print("   !", b)
    return not bad


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-dir", default=C.DEFAULT_LOG_DIR)
    ap.add_argument("--data-dir", default=C.DEFAULT_DATA_DIR)
    ap.add_argument("--key", action="store_true", help="only show key coordination events")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--quiet", action="store_true", help="don't print the event list")
    a = ap.parse_args()
    events = load(a.log_dir)
    if not a.quiet:
        show(events, a.key)
    if a.verify:
        raise SystemExit(0 if verify(events, a.data_dir) else 1)


if __name__ == "__main__":
    main()
