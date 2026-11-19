#!/usr/bin/env python3
"""
CECS 327 - Milestone 4: Coordination node.

Every node is a separate OS process that:
  * keeps a Lamport logical clock; every message carries its timestamp
  * takes part in Bully leader election (highest live id wins)
  * acts as the centralized mutual-exclusion coordinator when it is leader
  * edits the shared playlist only while holding the distributed lock

Run one per terminal:
    python3 -m milestone4.node --id 1
    ...
    python3 -m milestone4.node --id 5

Ctrl+C on a node simulates a CRASH (process exits immediately, no goodbye
messages), which is exactly the failure the coordinator must survive.
"""
from __future__ import annotations

import argparse
import heapq
import os
import random
import signal
import socket
import threading
import time
from typing import Dict, List, Optional

from . import common as C
from . import shared_playlist as SP

QUIET = {"HEARTBEAT"}        # high-frequency messages: clock updates, not printed

SONGS = ["Neon Skyline", "Paper Lanterns", "Coastal Drive", "Midnight Static",
         "Golden Hour Loop", "Satellite Hearts", "Lo-Fi Rain", "Echo Valley",
         "Velvet Freeway", "Glass Orchard", "Northbound", "Static Bloom"]


class CoordNode:
    def __init__(self, node_id: int, nodes: Dict[int, int], data_dir: str = C.DEFAULT_DATA_DIR,
                 log_dir: str = C.DEFAULT_LOG_DIR, cs_time: float = C.CS_TIME,
                 color: Optional[bool] = None):
        self.id = node_id
        self.nodes = nodes
        self.port = nodes[node_id]
        self.data_dir = data_dir
        self.cs_time = cs_time
        self.log = C.EventLog(node_id, log_dir, color=color)
        # Recover the clock from stable storage so a restarted node never
        # re-uses timestamps it already issued.
        self.clock = C.LamportClock(self.log.last_lamport())

        self.mu = threading.RLock()
        self.running = True
        self.msg_seq = 0
        self.incarnation = self.clock.value      # makes msg ids unique across restarts

        # ---- election state ----
        self.leader: Optional[int] = None
        self.term = 0
        self.electing = False
        self.election_round = 0
        self.got_ok = False
        self.last_hb = time.monotonic()

        # ---- mutex: requester side ----
        self.work_lock = threading.Lock()        # one local edit at a time
        self.want = False
        self.req_ts: Optional[int] = None
        self.holding = False
        self.token: Optional[List[int]] = None
        self.grant_evt = threading.Event()

        # ---- mutex: coordinator side (only used while leader) ----
        self._reset_coordinator_state()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _reset_coordinator_state(self, recovering: bool = False) -> None:
        self.c_holder: Optional[dict] = None     # {"node": id, "token": [term, seq]}
        self.c_queue: list = []                   # heap of (req_ts, node_id)
        self.c_seq = 0
        self.c_released: set = set()
        self.c_recovering_until = time.monotonic() + C.RECOVERY_WINDOW if recovering else 0.0

    def _spawn(self, fn, *args) -> None:
        threading.Thread(target=fn, args=args, daemon=True).start()

    def _queue_str(self) -> str:
        if not self.c_queue:
            return "[]"
        return "[" + ", ".join(f"N{n}(ts={t})" for t, n in sorted(self.c_queue)) + "]"

    def _event(self, event: str, detail: str = "", **fields) -> int:
        """A local event: tick the clock and log it."""
        ts = self.clock.tick()
        self.log.log(ts, event, detail, **fields)
        return ts

    # ------------------------------------------------------------------
    # messaging (every message carries a Lamport timestamp)
    # ------------------------------------------------------------------
    def send(self, dst: int, mtype: str, **payload) -> bool:
        with self.mu:
            self.msg_seq += 1
            msg_id = f"N{self.id}.{self.incarnation}-{self.msg_seq}"
        ts = self.clock.send()
        msg = {"type": mtype, "src": self.id, "dst": dst, "ts": ts, "msg_id": msg_id, **payload}
        if mtype not in QUIET:
            extra = " ".join(f"{k}={v}" for k, v in payload.items())
            self.log.log(ts, "SEND", f"{mtype:<12} -> N{dst}  (msg ts={ts}) {extra}".rstrip(),
                         msg_type=mtype, msg_id=msg_id, peer=dst)
        try:
            reply = C.send_msg(self.nodes[dst], msg)
            with self.mu:
                # acks piggyback the receiver's term so a restarted node catches up
                if reply.get("term", 0) > self.term:
                    self.term = reply["term"]
            return True
        except OSError:
            if mtype not in QUIET:
                self._event("SEND_FAIL", f"{mtype} -> N{dst} unreachable", msg_type=mtype, peer=dst)
            return False

    def broadcast(self, mtype: str, **payload) -> List[int]:
        """Send to every other node in parallel; returns the ids that were reachable."""
        reached, threads = [], []

        def one(p):
            if self.send(p, mtype, **payload):
                reached.append(p)

        for p in self.nodes:
            if p != self.id:
                t = threading.Thread(target=one, args=(p,), daemon=True)
                t.start()
                threads.append(t)
        for t in threads:
            t.join()
        return sorted(reached)

    # ------------------------------------------------------------------
    # server
    # ------------------------------------------------------------------
    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((C.HOST, self.port))
        srv.listen(64)
        self._srv = srv
        recovered = self.clock.value
        self._event("NODE_START", f"listening on {C.HOST}:{self.port}"
                    + (f" (recovered Lamport clock {recovered} from stable log)" if recovered else ""))
        self._spawn(self._accept_loop)
        reached = self.broadcast("HELLO")
        self._event("JOINED", f"reachable peers: {['N%d' % n for n in reached] or '(none)'}; known term={self.term}")
        self._spawn(self._monitor_loop)
        self.start_election("node startup")

    def _accept_loop(self) -> None:
        while self.running:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                break
            self._spawn(self._handle_conn, conn)

    def _handle_conn(self, conn: socket.socket) -> None:
        with conn:
            try:
                conn.settimeout(C.NET_TIMEOUT)
                msg = C.read_frame(conn)
                reply = self.handle(msg)
                C.write_frame(conn, reply or {"ack": True})
            except (OSError, ValueError):
                pass

    def handle(self, msg: dict) -> dict:
        mtype = msg.get("type", "")
        if mtype.startswith("CLIENT_"):
            return self._handle_client(msg)

        src = msg["src"]
        before, after = self.clock.recv(msg["ts"])
        if mtype not in QUIET:
            self.log.log(after, "RECV",
                         f"{mtype:<12} <- N{src}  (msg ts={msg['ts']}, clock {before} -> max({before},{msg['ts']})+1 = {after})",
                         msg_type=mtype, msg_id=msg.get("msg_id"), peer=src, msg_ts=msg["ts"])

        handler = getattr(self, f"_on_{mtype.lower()}", None)
        if handler:
            handler(msg)
        with self.mu:
            return {"ack": True, "term": self.term}

    # ------------------------------------------------------------------
    # failure detector + heartbeats
    # ------------------------------------------------------------------
    def _monitor_loop(self) -> None:
        last_beat = 0.0
        while self.running:
            time.sleep(C.HB_INTERVAL / 2)
            now = time.monotonic()
            with self.mu:
                leader, electing, last_hb, term = self.leader, self.electing, self.last_hb, self.term
            if leader == self.id:
                if now - last_beat >= C.HB_INTERVAL:
                    last_beat = now
                    reached = self.broadcast("HEARTBEAT", term=term)
                    self._check_holder_alive(reached)
            elif not electing and leader is not None and now - last_hb > C.HB_TIMEOUT:
                self._event("LEADER_FAILED",
                            f"no heartbeat from coordinator N{leader} for {now - last_hb:.1f}s -> starting election")
                with self.mu:
                    if self.leader == leader:
                        self.leader = None
                self.start_election(f"coordinator N{leader} failed")
            elif not electing and leader is None:
                self.start_election("no known coordinator")

    def _on_hello(self, msg: dict) -> None:
        pass   # logged by handle(); the ack carries our term back to the new node

    def _on_heartbeat(self, msg: dict) -> None:
        with self.mu:
            if msg["src"] == self.leader:
                self.last_hb = time.monotonic()
            elif msg["term"] > self.term and msg["src"] > self.id:
                # We missed a COORDINATOR announcement; adopt the newer leader.
                self.leader, self.term, self.last_hb = msg["src"], msg["term"], time.monotonic()

    # ------------------------------------------------------------------
    # Bully election
    # ------------------------------------------------------------------
    def start_election(self, reason: str) -> None:
        with self.mu:
            if self.electing or not self.running:
                return
            self.electing = True
            self.got_ok = False
            self.election_round += 1
            rnd = self.election_round
        self._spawn(self._run_election, reason, rnd)

    def _run_election(self, reason: str, rnd: int) -> None:
        higher = [n for n in sorted(self.nodes) if n > self.id]
        self._event("ELECTION_START",
                    f"reason: {reason}; sending ELECTION to higher ids {['N%d' % n for n in higher] or '(none)'}")
        threads = [threading.Thread(target=self.send, args=(n, "ELECTION"), daemon=True) for n in higher]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        deadline = time.monotonic() + C.ELECTION_OK_TIMEOUT
        while time.monotonic() < deadline:
            with self.mu:
                if self.got_ok or self.election_round != rnd or not self.electing:
                    break
            time.sleep(0.05)

        with self.mu:
            if self.election_round != rnd or not self.electing:
                return
            got_ok = self.got_ok
        if not got_ok:
            self._become_leader()
            return

        self._event("ELECTION_WAIT", "a higher node answered OK; waiting for its COORDINATOR message")
        deadline = time.monotonic() + C.COORDINATOR_TIMEOUT
        while time.monotonic() < deadline:
            with self.mu:
                if not self.electing or self.election_round != rnd:
                    return
            time.sleep(0.05)
        with self.mu:
            if self.election_round != rnd or not self.electing:
                return
            self.electing = False
        self.start_election("timed out waiting for COORDINATOR")

    def _on_election(self, msg: dict) -> None:
        # A lower node is holding an election: bully it with OK, then run our own.
        self._spawn(self.send, msg["src"], "OK")
        self.start_election(f"received ELECTION from lower node N{msg['src']}")

    def _on_ok(self, msg: dict) -> None:
        with self.mu:
            self.got_ok = True

    def _become_leader(self) -> None:
        with self.mu:
            prev = self.leader
            self.term += 1
            self.leader = self.id
            self.electing = False
            term = self.term
            if prev != self.id:
                # Fresh coordinator: lock state from the old coordinator is
                # unknown, so collect it from the nodes before granting.
                self._reset_coordinator_state(recovering=True)
        self._event("LEADER_ELECTED",
                    f"N{self.id} is the coordinator (term {term}); no higher node answered")
        self.broadcast("COORDINATOR", term=term)
        if prev != self.id:
            self._event("RECOVERY", f"collecting lock state for {C.RECOVERY_WINDOW:.1f}s before granting")
            self._spawn(self._finish_recovery, term)
            self._spawn(self._report_lock_state, self.id)

    def _on_coordinator(self, msg: dict) -> None:
        src, term = msg["src"], msg["term"]
        if src < self.id:
            # Bully rule: a lower node may not lead while a higher node is alive.
            with self.mu:
                self.term = max(self.term, term)   # keep terms unique for fencing tokens
            self._event("BULLY", f"rejecting COORDINATOR from lower node N{src}; N{self.id} outranks it")
            self.start_election(f"lower node N{src} claimed leadership")
            return
        with self.mu:
            if term < self.term:
                stale = True
            else:
                stale = False
                prev = self.leader
                self.leader, self.term = src, term
                self.electing = False
                self.last_hb = time.monotonic()
                if prev == self.id and src != self.id:
                    self._reset_coordinator_state()
        if stale:
            self._event("STALE_COORD", f"ignored COORDINATOR from N{src} (term {term} < {self.term})")
            return
        self._event("LEADER_ACCEPTED", f"coordinator is N{src} (term {term})")
        self._spawn(self._report_lock_state, src)

    def _report_lock_state(self, leader: int) -> None:
        """After a COORDINATOR announcement, a lock holder tells the coordinator
        it still holds the lock. (Waiting nodes re-send REQUEST from acquire().)"""
        with self.mu:
            holding, token = self.holding, self.token
        if holding:
            self.send(leader, "STATE_REPORT", holding=True, token=token)

    # ------------------------------------------------------------------
    # Centralized mutual exclusion: COORDINATOR side
    # ------------------------------------------------------------------
    def _on_request(self, msg: dict) -> None:
        src, req_ts = msg["src"], msg["req_ts"]
        regrant = None
        with self.mu:
            if self.leader != self.id:
                ignored = True
            else:
                ignored = False
                if self.c_holder and self.c_holder["node"] == src:
                    regrant = self.c_holder["token"]       # GRANT was lost; resend it
                elif any(n == src for _, n in self.c_queue):
                    pass                                   # duplicate
                else:
                    heapq.heappush(self.c_queue, (req_ts, src))
                busy = self.c_holder is not None
                holder = f"N{self.c_holder['node']}" if busy else "none"
                q = self._queue_str()
                recovering = time.monotonic() < self.c_recovering_until
        if ignored:
            self._event("NOT_LEADER", f"REQUEST from N{src} ignored: N{self.id} is not the coordinator")
            return
        if regrant:
            self._spawn(self.send, src, "GRANT", token=regrant)
            return
        if busy or recovering:
            why = f"lock held by {holder}" if busy else "recovering lock state"
            self._event("LOCK_QUEUED", f"N{src} (req_ts={req_ts}) must wait: {why}; queue={q}")
        self._spawn(self._try_grant)

    def _try_grant(self) -> None:
        while True:
            with self.mu:
                if (self.leader != self.id or self.c_holder is not None or not self.c_queue
                        or time.monotonic() < self.c_recovering_until):
                    return
                req_ts, node = heapq.heappop(self.c_queue)
                self.c_seq += 1
                token = [self.term, self.c_seq]
                self.c_holder = {"node": node, "token": token}
                q = self._queue_str()
            self._event("LOCK_GRANTED",
                        f"granting lock to N{node} (smallest req_ts={req_ts}), fencing token={token}; waiting={q}")
            if self.send(node, "GRANT", token=token):
                return
            with self.mu:
                if self.c_holder and self.c_holder["token"] == token:
                    self.c_holder = None
            self._event("HOLDER_FAILED", f"N{node} unreachable, skipping to next in queue")

    def _on_release(self, msg: dict) -> None:
        token = msg["token"]
        with self.mu:
            self.c_released.add(tuple(token))
            ok = bool(self.c_holder and self.c_holder["token"] == token)
            if ok:
                self.c_holder = None
            q = self._queue_str()
        if ok:
            self._event("LOCK_RELEASED", f"N{msg['src']} released token {token}; waiting={q}")
        self._try_grant()

    def _on_state_report(self, msg: dict) -> None:
        token = msg["token"]
        with self.mu:
            if self.leader != self.id or tuple(token) in self.c_released:
                return
            current = self.c_holder
            if current is None:
                self.c_holder = {"node": msg["src"], "token": token}
        if current is None:
            self._event("RECOVERED", f"N{msg['src']} still holds the lock (token {token}); it keeps it until RELEASE")
        elif current["token"] != token:
            self._event("LATE_REPORT", f"N{msg['src']} reports stale token {token}; current holder N{current['node']} "
                                       f"- fencing will reject the stale write")

    def _finish_recovery(self, term: int) -> None:
        time.sleep(C.RECOVERY_WINDOW + 0.05)
        with self.mu:
            if self.leader != self.id or self.term != term:
                return
            holder = f"N{self.c_holder['node']}" if self.c_holder else "none"
            q = self._queue_str()
        self._event("RECOVERY_DONE", f"lock holder={holder}, waiting={q}; resuming grants")
        self._try_grant()

    def _check_holder_alive(self, reachable: List[int]) -> None:
        with self.mu:
            h = self.c_holder
            dead = h is not None and h["node"] != self.id and h["node"] not in reachable
            if dead:
                self.c_released.add(tuple(h["token"]))
                self.c_holder = None
        if dead:
            self._event("HOLDER_FAILED",
                        f"lock holder N{h['node']} crashed; revoking token {h['token']} (fencing blocks any late write)")
            self._try_grant()

    # ------------------------------------------------------------------
    # Centralized mutual exclusion: REQUESTER side
    # ------------------------------------------------------------------
    def acquire(self) -> None:
        ts = self.clock.tick()
        with self.mu:
            self.want, self.holding, self.req_ts = True, False, ts
            self.grant_evt.clear()
        self.log.log(ts, "CS_REQUEST", f"want exclusive access to the shared playlist (req_ts={ts})")
        sent_to = None                      # (leader, term) the REQUEST was delivered to
        while self.running:
            with self.mu:
                target = (self.leader, self.term)
            if target[0] is not None and target != sent_to:
                # first attempt, or a new coordinator took over: (re-)send REQUEST
                # with the ORIGINAL timestamp so we keep our place in line.
                if self.send(target[0], "REQUEST", req_ts=ts):
                    sent_to = target
                else:
                    self.start_election(f"coordinator N{target[0]} unreachable")
            if self.grant_evt.wait(0.2):
                return

    def _on_grant(self, msg: dict) -> None:
        with self.mu:
            accept = self.want and not self.holding
            if accept:
                self.want, self.holding, self.token = False, True, msg["token"]
        if accept:
            self.grant_evt.set()
        else:
            # Not expecting a grant (duplicate / stale) - hand it straight back.
            self._spawn(self.send, msg["src"], "RELEASE", token=msg["token"])

    def release(self) -> None:
        with self.mu:
            token, self.token, self.holding = self.token, None, False
            leader = self.leader
        if leader is not None and not self.send(leader, "RELEASE", token=token):
            self._event("RELEASE_LOST", f"coordinator N{leader} unreachable; new coordinator will not see me as holder")

    # ------------------------------------------------------------------
    # The critical section: edit the shared playlist
    # ------------------------------------------------------------------
    def add_track(self, title: str, unsafe: bool = False, hold: Optional[float] = None) -> None:
        with self.work_lock:
            if unsafe:
                self._event("UNSAFE_MODE", "skipping mutual exclusion (to show what goes wrong)")
                self._critical_section(title, unsafe=True, hold=hold)
                return
            self.acquire()
            try:
                self._critical_section(title, unsafe=False, hold=hold)
            finally:
                self.release()

    def _critical_section(self, title: str, unsafe: bool, hold: Optional[float]) -> None:
        mode = "unsafe" if unsafe else "safe"
        path = SP.path_for(self.data_dir, unsafe)
        data = SP.read(path)
        v0 = data["version"]
        token = None if unsafe else self.token
        self._event("CS_ENTER",
                    f"[{mode}] read playlist v{v0} ({len(data['tracks'])} tracks)"
                    + (f", fencing token={token}" if token else ""), mode=mode)

        time.sleep(self.cs_time if hold is None else hold)   # simulated work

        current = SP.read(path)
        if current["version"] != v0:
            self._event("CONFLICT",
                        f"[{mode}] playlist changed v{v0} -> v{current['version']} by another node while I was "
                        f"editing; my write overwrites it", mode=mode)
        if not unsafe and not SP.token_ok(token, current["last_token"]):
            self._event("FENCED", f"write rejected: token {token} <= last {current['last_token']}", mode=mode)
            return
        ts = self.clock.tick()
        data["tracks"].append({"title": title, "added_by": f"N{self.id}", "lamport": ts,
                               "token": token, "wall": C.wall_iso()})
        data["version"] = v0 + 1
        if token:
            data["last_token"] = token
        SP.write(path, data)
        self.log.log(ts, "CS_EXIT",
                     f"[{mode}] added '{title}', wrote playlist v{v0 + 1} ({len(data['tracks'])} tracks)",
                     mode=mode)

    # ------------------------------------------------------------------
    # client / operator commands (not part of the protocol)
    # ------------------------------------------------------------------
    def _handle_client(self, msg: dict) -> dict:
        mtype = msg["type"]
        if mtype == "CLIENT_STATUS":
            with self.mu:
                return {
                    "id": self.id, "leader": self.leader, "term": self.term, "clock": self.clock.value,
                    "electing": self.electing, "want": self.want, "holding": self.holding,
                    "coordinator": None if self.leader != self.id else {
                        "holder": self.c_holder, "queue": sorted(self.c_queue),
                        "recovering": time.monotonic() < self.c_recovering_until},
                }
        if mtype == "CLIENT_ADD":
            title = msg.get("title") or f"{random.choice(SONGS)} (via N{self.id})"
            self._event("CLIENT_CMD", f"user asked to add '{title}'" + (" [UNSAFE]" if msg.get("unsafe") else ""))
            self._spawn(self.add_track, title, bool(msg.get("unsafe")), msg.get("hold"))
            return {"accepted": True, "title": title}
        if mtype == "CLIENT_CRASH":
            self.crash("crash requested by operator")
            return {"crashing": True}
        return {"error": f"unknown command {mtype}"}

    def crash(self, why: str, event: str = "CRASH") -> None:
        self.running = False
        self._event(event, f"{why}: exiting immediately (no goodbye messages)")
        self.log.close()

        def die():
            time.sleep(0.05)
            os._exit(1)

        threading.Thread(target=die, daemon=True).start()


def main() -> None:
    ap = argparse.ArgumentParser(description="Milestone 4 coordination node")
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--nodes", help="cluster, e.g. 1:9101,2:9102,3:9103 (default: 5 nodes on 9101-9105)")
    ap.add_argument("--data-dir", default=C.DEFAULT_DATA_DIR)
    ap.add_argument("--log-dir", default=C.DEFAULT_LOG_DIR)
    ap.add_argument("--cs-time", type=float, default=C.CS_TIME)
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()

    node = CoordNode(args.id, C.parse_nodes(args.nodes), args.data_dir, args.log_dir,
                     args.cs_time, color=False if args.no_color else None)

    def on_signal(signum, frame):
        if signum == signal.SIGINT:
            node.crash("Ctrl+C")
        else:
            node.crash("terminated by demo/OS", event="SHUTDOWN")
        time.sleep(0.2)
        os._exit(1)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    node.start()
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
