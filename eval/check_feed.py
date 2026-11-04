"""
Runs an eval scenario against a fresh service while a client follows /alarms/stream
like a browser's EventSource, then checks that every alarm in the log reached the
client exactly once, in ingest order per room, and how long it took (p50/p95/max).

--kill-at N hard-kills the service after N seconds and restarts it. The client
reconnects with Last-Event-ID, so nothing ingested before the kill may go missing.

    uv run python eval/check_feed.py offline --devices 5000 --kill-at 60
"""

import argparse
import json
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "data" / "events.jsonl"


def service_is_up():
    try:
        urlopen("http://localhost:8080/metrics", timeout=1)
        return True
    except OSError:
        return False


def start_service():
    service = subprocess.Popen(["uv", "run", "python", "-m", "service.main"], cwd=ROOT,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    while not service_is_up():
        time.sleep(0.1)
    return service


def kill_and_restart(services, kill_at):
    time.sleep(kill_at)
    services[-1].kill()  # SIGKILL, no shutdown
    while service_is_up():
        time.sleep(0.05)
    down = time.time()
    services.append(start_service())
    print(f"\n[feed check] hard-killed the service at {kill_at:.0f}s, back up in {time.time() - down:.1f}s\n",
          flush=True)


class FeedClient:
    """Follows the feed like EventSource: remembers the last id and reconnects with it."""

    def __init__(self):
        self.received = []  # (received_ms, alarm) in delivery order
        self.last_id = None
        self.connections = 0
        self.resumes = 0

    def follow(self):
        while True:
            try:
                self.read_stream()
            except OSError:
                pass
            time.sleep(0.1)

    def read_stream(self):
        sock = socket.create_connection(("localhost", 8080))
        resume = f"Last-Event-ID: {self.last_id}\r\n" if self.last_id else ""
        sock.sendall(f"GET /alarms/stream HTTP/1.1\r\nHost: localhost\r\n{resume}\r\n".encode())
        self.connections += 1
        self.resumes += bool(resume)
        event_id = data = None
        for line in sock.makefile("rb"):
            if line.startswith(b"id: "):
                event_id = line[4:].strip().decode()
            elif line.startswith(b"data: "):
                data = json.loads(line[6:])
            elif line == b"\n" and data:  # a blank line ends the message
                self.received.append((time.time() * 1000, data))
                self.last_id, data = event_id, None


def ingested_alarms():
    """First arrival of each distinct fall in the log, in log order: alarm id -> (arrival_ms, room_id, ts)."""
    first = {}
    for line in LOG.read_bytes().splitlines():
        if b'"fall_warn"' in line:
            arrival, device_id, room_id, _, ts, _ = json.loads(line)
            first.setdefault(f"{device_id}:{ts}", (arrival, room_id, ts))
    return first


def report(scenario, ingested, client):
    first_received = {}
    for received_ms, alarm in client.received:
        first_received.setdefault(alarm["event_id"], received_ms)
    delivered = first_received.keys() & ingested.keys()
    latencies = [first_received[i] - ingested[i][0] for i in delivered]

    ingested_by_room, delivered_by_room = defaultdict(list), defaultdict(list)
    for alarm_id, (_, room_id, _) in ingested.items():
        ingested_by_room[room_id].append(alarm_id)
    for alarm_id in first_received:
        if alarm_id in ingested:
            delivered_by_room[ingested[alarm_id][1]].append(alarm_id)
    same_order = sum(delivered_by_room[room] == ids for room, ids in ingested_by_room.items())

    behind_newer = 0  # falls delivered after a fall with a later ts from the same room
    for ids in delivered_by_room.values():
        newest = 0
        for alarm_id in ids:
            ts = ingested[alarm_id][2]
            behind_newer += ts < newest
            newest = max(newest, ts)

    print(f"\n=== Feed check: {scenario} ===")
    print(f"  alarms ingested       {len(ingested)}")
    print(f"  delivered             {len(delivered)}")
    print(f"  missing               {len(ingested.keys() - first_received.keys())}")
    print(f"  duplicates            {len(client.received) - len(first_received)}")
    print(f"  unknown ids           {len(first_received.keys() - ingested.keys())}")
    print(f"  connections           {client.connections} ({client.resumes} resumed with Last-Event-ID)")
    print(f"  ingestion order       kept in {same_order} of {len(ingested_by_room)} rooms")
    print(f"  ts order              {behind_newer} falls came after a later-ts fall in their room")
    if len(latencies) > 1:
        q = statistics.quantiles(latencies, n=100)
        print(f"  ingest to client      p50 {q[49]:.0f}ms  p95 {q[94]:.0f}ms  max {max(latencies):.0f}ms")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=["smoke", "baseline", "burst", "offline", "adversarial"])
    parser.add_argument("--devices", type=int, default=5000)
    parser.add_argument("--kill-at", type=float, help="seconds in to hard-kill and restart the service")
    args = parser.parse_args()

    if service_is_up():
        sys.exit("Stop the service on :8080 first; this check starts its own.")
    for path in (LOG, LOG.with_name("snapshot.pickle")):
        path.unlink(missing_ok=True)
    services = [start_service()]
    client = FeedClient()
    threading.Thread(target=client.follow, daemon=True).start()
    if args.kill_at:
        threading.Thread(target=kill_and_restart, args=(services, args.kill_at), daemon=True).start()

    subprocess.run([sys.executable, ROOT / "eval" / "check.py", args.scenario, "--devices", str(args.devices)])
    time.sleep(1)  # let the last alarms reach the client
    services[-1].kill()
    while service_is_up():  # so the next run can start its own
        time.sleep(0.05)
    report(args.scenario, ingested_alarms(), client)


if __name__ == "__main__":
    main()
