"""
Recounts health and occupancy from the whole event log by brute force, then asks
each of the four workers for every room and device and compares.

Run it with the service still up, right after a scenario, so the 5-minute health
window still covers the run:

    uv run python eval/recount.py
"""

import http.client
import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "data" / "events.jsonl"
WINDOWS = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000}
WORKERS = 4


def read_log():
    """room -> its presence changes sorted by ts, and device -> its heartbeat ts."""
    presence = defaultdict(list)
    heartbeats = defaultdict(list)
    for arrival_order, line in enumerate(LOG.open("rb")):
        arrival, device, room, kind, ts, value = json.loads(line)
        presence.setdefault(room, [])  # rooms with no presence yet are checked too
        if kind == "presence":
            presence[room].append((ts, arrival_order, value))
        elif kind == "heartbeat":
            heartbeats[device].append(ts)
    for changes in presence.values():
        changes.sort()
    return presence, heartbeats


def expected_occupancy(changes, window, now):
    window_start = now - WINDOWS[window]
    occupied_ms = 0
    for i, (ts, _, in_room) in enumerate(changes):
        next_ts = changes[i + 1][0] if i + 1 < len(changes) else now
        if in_room:
            occupied_ms += max(0, min(next_ts, now) - max(ts, window_start))
    return (bool(changes[-1][2]) if changes else False), occupied_ms / WINDOWS[window]


def expected_health(heartbeats, now):
    now_second = now // 1000
    seconds = {ts // 1000 for ts in heartbeats}
    recent = [second for second in seconds if now_second - 300 < second <= now_second]
    last = max(heartbeats)
    last_iso = datetime.fromtimestamp(last / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return last_iso, round(len(recent) / 300, 4)


def get(connection, path):
    """The answer, bracketed by our clock before and after the call."""
    before = time.time_ns() // 1_000_000
    connection.request("GET", path)
    answer = json.loads(connection.getresponse().read())
    return answer, before, time.time_ns() // 1_000_000


def connection_to_each_worker():
    """Workers share the port, so keep opening connections until each worker has one."""
    connections = {}
    while len(connections) < WORKERS:
        connection = http.client.HTTPConnection("localhost", 8080)
        connections.setdefault(get(connection, "/metrics")[0]["worker"], connection)
    return connections


def main():
    presence, heartbeats = read_log()
    mismatches = defaultdict(list)
    for worker, connection in connection_to_each_worker().items():
        for room, changes in presence.items():
            for window in WINDOWS:
                served, before, after = get(connection, f"/rooms/{room}/occupancy?window={window}")
                in_room, occupied_pct = expected_occupancy(changes, window, (before + after) // 2)
                # The clock moves while we ask: 0.0002 of a minute is 12ms.
                if served["in_room"] != in_room or abs(served["occupied_pct"] - occupied_pct) > 0.0002:
                    mismatches["occupancy"].append((worker, room, window, served, round(occupied_pct, 4)))
        for device, device_heartbeats in heartbeats.items():
            served, before, after = get(connection, f"/devices/{device}/health")
            expected = {expected_health(device_heartbeats, before), expected_health(device_heartbeats, after)}
            if (served["last_heartbeat_ts"], served["availability_5m"]) not in expected:
                mismatches["health"].append((worker, device, served, expected))

    answers = WORKERS * (len(presence) * len(WINDOWS) + len(heartbeats))
    print(f"{len(presence)} rooms x {len(WINDOWS)} windows and {len(heartbeats)} devices, "
          f"on each of {WORKERS} workers: {answers} answers")
    for kind in ("occupancy", "health"):
        print(f"  {kind} mismatches: {len(mismatches[kind])}")
        for mismatch in mismatches[kind][:5]:
            print("   ", mismatch)


if __name__ == "__main__":
    main()
