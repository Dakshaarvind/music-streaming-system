"""Runs the Milestone 1 demo requests against a running gateway.

Usage:
    python scripts/demo.py [gateway_url]

Prints each request's correlation ID so it can be fed into
scripts/trace.py <request_id> to see the full multi-hop path.
"""
import sys
import uuid

import httpx

GATEWAY_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


def call(method: str, path: str, **kwargs) -> None:
    request_id = str(uuid.uuid4())
    headers = {"X-Request-ID": request_id}
    print(f"\n=== {method} {path} (request_id={request_id}) ===")

    response = httpx.request(method, f"{GATEWAY_URL}{path}", headers=headers, **kwargs)

    print(f"status: {response.status_code}")
    print(f"body:   {response.text}")
    print(f"trace:  python scripts/trace.py {request_id}")


def main():
    print(f"Gateway: {GATEWAY_URL}")

    # 1. Happy path: fetch user 42's playlists (fans out to user + playlist + catalog).
    call("GET", "/users/42/playlists")

    # 2. Create a new playlist for user 42 (fans out to user + playlist).
    call(
        "POST",
        "/users/42/playlists",
        json={"name": "Road Trip", "track_ids": [1, 2]},
    )

    # 3. Unknown user -> 404 from the gateway.
    call("GET", "/users/9999/playlists")

    # 4. Direct catalog lookup through the gateway.
    call("GET", "/tracks/1")


if __name__ == "__main__":
    main()
