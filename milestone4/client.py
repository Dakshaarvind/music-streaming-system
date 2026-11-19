#!/usr/bin/env python3
"""
Operator / user client for the Milestone 4 cluster.

    python3 -m milestone4.client status                 # leader, term, clock, lock state of every node
    python3 -m milestone4.client add --node 2           # one node adds a track (uses the distributed lock)
    python3 -m milestone4.client burst                  # ALL live nodes add a track at the same instant
    python3 -m milestone4.client burst --unsafe         # same, but with mutual exclusion switched off
    python3 -m milestone4.client playlist [--unsafe]    # print the shared playlist file
    python3 -m milestone4.client crash --node 5         # crash a node (same as Ctrl+C in its terminal)
    python3 -m milestone4.client reset                  # wipe playlists + logs (run BEFORE starting nodes)
"""
from __future__ import annotations

import argparse
import os
import shutil
import threading
from typing import Dict, List, Optional

from . import common as C
from . import shared_playlist as SP


def call(nodes: Dict[int, int], nid: int, mtype: str, **payload) -> Optional[dict]:
    try:
        return C.send_msg(nodes[nid], {"type": mtype, **payload})
    except OSError:
        return None


def status(nodes: Dict[int, int]) -> Dict[int, Optional[dict]]:
    return {nid: call(nodes, nid, "CLIENT_STATUS") for nid in sorted(nodes)}


def alive(nodes: Dict[int, int]) -> List[int]:
    return [n for n, s in status(nodes).items() if s is not None]


def print_status(nodes: Dict[int, int]) -> None:
    print(f"{'node':<6}{'state':<9}{'leader':<8}{'term':<6}{'lamport':<9}{'mutex':<10}coordinator view")
    print("-" * 90)
    for nid, s in status(nodes).items():
        if s is None:
            print(f"N{nid:<5}{'DOWN':<9}")
            continue
        role = "LEADER" if s["leader"] == nid else ("electing" if s["electing"] else "follower")
        mutex = "HOLDING" if s["holding"] else ("waiting" if s["want"] else "-")
        coord = ""
        if s["coordinator"]:
            c = s["coordinator"]
            h = f"N{c['holder']['node']} token={c['holder']['token']}" if c["holder"] else "free"
            q = ", ".join(f"N{n}(ts={t})" for t, n in c["queue"]) or "empty"
            coord = f"lock={h}; queue=[{q}]" + ("; RECOVERING" if c["recovering"] else "")
        leader = f"N{s['leader']}" if s["leader"] else "?"
        print(f"N{nid:<5}{role:<9}{leader:<8}{s['term']:<6}{s['clock']:<9}{mutex:<10}{coord}")


def add(nodes, nid, title=None, unsafe=False, hold=None) -> Optional[dict]:
    return call(nodes, nid, "CLIENT_ADD", title=title, unsafe=unsafe, hold=hold)


def burst(nodes, unsafe=False, hold=None, only: Optional[List[int]] = None) -> Dict[int, Optional[dict]]:
    """Fire CLIENT_ADD at every live node simultaneously (released by one barrier)."""
    targets = only or alive(nodes)
    barrier = threading.Barrier(len(targets))
    results: Dict[int, Optional[dict]] = {}

    def go(n):
        barrier.wait()
        results[n] = add(nodes, n, unsafe=unsafe, hold=hold)

    threads = [threading.Thread(target=go, args=(n,)) for n in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def print_playlist(data_dir: str, unsafe: bool = False) -> dict:
    data = SP.read(SP.path_for(data_dir, unsafe))
    label = "UNSAFE playlist (no mutual exclusion)" if unsafe else "Shared playlist (protected)"
    print(f"{label}: '{data['name']}'  version={data['version']}  tracks={len(data['tracks'])}"
          f"  last_token={data['last_token']}")
    for i, t in enumerate(data["tracks"], 1):
        print(f"  {i:>2}. {t['title']:<36} by {t['added_by']:<4} lamport={t['lamport']:<5} token={t['token']}")
    return data


def reset(data_dir: str, log_dir: str) -> None:
    shutil.rmtree(log_dir, ignore_errors=True)
    os.makedirs(log_dir, exist_ok=True)
    SP.reset(data_dir)
    print(f"Reset: cleared {log_dir} and emptied playlists in {data_dir}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Milestone 4 client")
    ap.add_argument("cmd", choices=["status", "add", "burst", "playlist", "crash", "reset"])
    ap.add_argument("--node", type=int)
    ap.add_argument("--title")
    ap.add_argument("--unsafe", action="store_true", help="bypass mutual exclusion")
    ap.add_argument("--hold", type=float, help="seconds to stay in the critical section")
    ap.add_argument("--nodes", help="cluster spec, e.g. 1:9101,2:9102")
    ap.add_argument("--data-dir", default=C.DEFAULT_DATA_DIR)
    ap.add_argument("--log-dir", default=C.DEFAULT_LOG_DIR)
    a = ap.parse_args()
    nodes = C.parse_nodes(a.nodes)

    if a.cmd == "status":
        print_status(nodes)
    elif a.cmd == "add":
        if a.node is None:
            ap.error("add needs --node")
        r = add(nodes, a.node, a.title, a.unsafe, a.hold)
        print(f"N{a.node}: {r}" if r else f"N{a.node} is down")
    elif a.cmd == "burst":
        for n, r in sorted(burst(nodes, a.unsafe, a.hold).items()):
            print(f"N{n}: {r['title'] if r else 'down'}")
    elif a.cmd == "playlist":
        print_playlist(a.data_dir, a.unsafe)
    elif a.cmd == "crash":
        if a.node is None:
            ap.error("crash needs --node")
        print(f"N{a.node}: {call(nodes, a.node, 'CLIENT_CRASH') or 'already down'}")
    elif a.cmd == "reset":
        reset(a.data_dir, a.log_dir)


if __name__ == "__main__":
    main()
