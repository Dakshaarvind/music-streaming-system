"""Reconstructs one request's full path across services from log files.

Usage:
    python scripts/trace.py <request_id>

Merges every logs/<service>.log file, filters to the given request_id,
and prints the matching lines in timestamp order -- showing the request's
full journey through the gateway, user-service, playlist-service, and
catalog-service.
"""
import glob
import json
import sys


def load_matching_records(request_id: str) -> list[dict]:
    records = []
    for log_path in glob.glob("logs/*.log"):
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("request_id") == request_id:
                    records.append(record)
    return records


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/trace.py <request_id>")
        sys.exit(1)

    request_id = sys.argv[1]
    records = load_matching_records(request_id)

    if not records:
        print(f"No log entries found for request_id={request_id}")
        sys.exit(1)

    records.sort(key=lambda r: r["timestamp"])

    print(f"Trace for request_id={request_id} ({len(records)} events)\n")
    for r in records:
        line = (
            f"{r['timestamp']}  {r['service']:<18} {r['direction']:<14} "
            f"peer={r.get('peer', '-'):<18} {r.get('method', ''):<5} {r.get('path', '')}"
        )
        if "status" in r:
            line += f"  status={r['status']} latency_ms={r.get('latency_ms')}"
        print(line)


if __name__ == "__main__":
    main()
