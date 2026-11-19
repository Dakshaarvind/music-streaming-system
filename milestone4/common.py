"""
Shared building blocks for Milestone 4:
  - cluster configuration (node ids -> TCP ports, timing constants)
  - LamportClock (logical clock with the three Lamport update rules)
  - length-prefixed JSON transport over TCP (same framing as Milestones 2/3)
  - EventLog: console + JSON-lines event log, one file per node
"""
from __future__ import annotations

import json
import os
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Optional

HOST = "127.0.0.1"
BASE_PORT = 9100                      # node i listens on BASE_PORT + i
DEFAULT_NODE_IDS = [1, 2, 3, 4, 5]

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LOG_DIR = os.path.join(ROOT, "logs", "m4")
DEFAULT_DATA_DIR = os.path.join(ROOT, "data", "m4")

# Timing (seconds). Small values keep the demo snappy on localhost.
HB_INTERVAL = 0.5          # leader -> followers heartbeat period
HB_TIMEOUT = 1.5           # follower declares leader dead after this silence
ELECTION_OK_TIMEOUT = 0.8  # wait for OK from a higher node
COORDINATOR_TIMEOUT = 3.0  # after an OK, wait this long for COORDINATOR
RECOVERY_WINDOW = 1.0      # new coordinator collects lock state before granting
CS_TIME = 0.6              # simulated work inside the critical section
NET_TIMEOUT = 1.0          # TCP connect/read timeout


def parse_nodes(spec: Optional[str]) -> Dict[int, int]:
    """'1:9101,2:9102' -> {1: 9101, 2: 9102}. None -> default 5-node cluster."""
    if not spec:
        return {i: BASE_PORT + i for i in DEFAULT_NODE_IDS}
    out = {}
    for part in spec.split(","):
        nid, port = part.split(":")
        out[int(nid)] = int(port)
    return out


def nodes_to_spec(nodes: Dict[int, int]) -> str:
    return ",".join(f"{i}:{p}" for i, p in sorted(nodes.items()))


def wall_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# =====================================================================
# Lamport logical clock
# =====================================================================
class LamportClock:
    """
    Rules (Lamport 1978):
      1. Local event:  C = C + 1
      2. Send:         C = C + 1, message carries ts = C
      3. Receive(ts):  C = max(C, ts) + 1
    """

    def __init__(self, start: int = 0):
        self._value = start
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def send(self) -> int:
        return self.tick()

    def recv(self, msg_ts: int):
        """Returns (clock_before, clock_after)."""
        with self._lock:
            before = self._value
            self._value = max(self._value, int(msg_ts)) + 1
            return before, self._value


# =====================================================================
# Transport: 4-byte big-endian length header + UTF-8 JSON body
# =====================================================================
def _recvall(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("peer closed connection")
        data.extend(chunk)
    return bytes(data)


def write_frame(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def read_frame(sock: socket.socket) -> dict:
    (length,) = struct.unpack(">I", _recvall(sock, 4))
    return json.loads(_recvall(sock, length).decode("utf-8"))


def send_msg(port: int, msg: dict, timeout: float = NET_TIMEOUT) -> dict:
    """Request/ack over a fresh TCP connection. Raises OSError if unreachable."""
    with socket.create_connection((HOST, port), timeout=timeout) as s:
        s.settimeout(timeout)
        write_frame(s, msg)
        return read_frame(s)


# =====================================================================
# Event log
# =====================================================================
_COLORS = {1: "\033[36m", 2: "\033[32m", 3: "\033[33m", 4: "\033[35m", 5: "\033[34m"}
_HIGHLIGHT = {
    "LEADER_ELECTED": "\033[1;97;42m",
    "LEADER_FAILED": "\033[1;97;41m",
    "CRASH": "\033[1;97;41m",
    "CONFLICT": "\033[1;97;41m",
    "LOST_UPDATE": "\033[1;97;41m",
    "CS_ENTER": "\033[1;30;43m",
    "CS_EXIT": "\033[1;30;43m",
    "LOCK_GRANTED": "\033[1m",
    "LOCK_QUEUED": "\033[1m",
    "HOLDER_FAILED": "\033[1;97;41m",
    "FENCED": "\033[1;97;41m",
}
_RESET = "\033[0m"


class EventLog:
    def __init__(self, node_id: int, log_dir: str, color: Optional[bool] = None):
        self.node_id = node_id
        os.makedirs(log_dir, exist_ok=True)
        self.path = os.path.join(log_dir, f"node-{node_id}.jsonl")
        self._lock = threading.Lock()
        self._fh = open(self.path, "a", buffering=1, encoding="utf-8")
        self._closed = False
        self.color = sys.stdout.isatty() if color is None else color

    def last_lamport(self) -> int:
        """Stable-storage recovery: highest Lamport value this node ever logged."""
        best = 0
        try:
            with open(self.path, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        best = max(best, int(json.loads(line).get("lamport", 0)))
                    except (ValueError, json.JSONDecodeError):
                        pass
        except FileNotFoundError:
            pass
        return best

    def log(self, lamport: int, event: str, detail: str = "", **fields) -> None:
        rec = {"lamport": lamport, "node": self.node_id, "wall": wall_iso(),
               "event": event, "detail": detail}
        rec.update(fields)
        line = json.dumps(rec, separators=(",", ":"))
        ts = rec["wall"][11:23]
        tag = f"N{self.node_id}"
        if self.color:
            c = _COLORS.get(self.node_id, "")
            hl = _HIGHLIGHT.get(event, "")
            text = (f"{c}[{ts}] [L={lamport:>4}] [{tag}]{_RESET} "
                    f"{hl}{event:<15}{_RESET} {detail}")
        else:
            text = f"[{ts}] [L={lamport:>4}] [{tag}] {event:<15} {detail}"
        with self._lock:
            if self._closed:
                return
            self._fh.write(line + "\n")
            print(text, flush=True)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._fh.flush()
            self._fh.close()
