"""
The shared resource: a collaborative playlist stored as one JSON file that
every node edits with a read-modify-write cycle.

Read-modify-write on a shared file is the classic race: if two nodes read
version v at the same time, both write v+1 and one track silently
disappears (a *lost update*). Coordination is therefore required.

Two protections:
  1. Mutual exclusion (the coordinator's lock) - only one writer at a time.
  2. Fencing token - every GRANT carries a monotonically increasing token
     (term, seq). The file remembers the highest token that wrote to it and
     rejects a write carrying an older token, so a node holding a stale lock
     from a dead coordinator can never clobber newer data.
"""
from __future__ import annotations

import json
import os
from typing import List, Optional

SAFE_FILE = "shared_playlist.json"
UNSAFE_FILE = "unsafe_playlist.json"


def path_for(data_dir: str, unsafe: bool) -> str:
    return os.path.join(data_dir, UNSAFE_FILE if unsafe else SAFE_FILE)


def empty() -> dict:
    return {"name": "Group Road-Trip Mix", "version": 0, "last_token": [0, 0], "tracks": []}


def read(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return empty()


def write(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    os.replace(tmp, path)          # atomic rename: readers never see half a file


def token_ok(new: Optional[List[int]], last: List[int]) -> bool:
    return new is not None and tuple(new) > tuple(last)


def reset(data_dir: str) -> None:
    for unsafe in (False, True):
        write(path_for(data_dir, unsafe), empty())
