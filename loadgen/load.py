"""
Load tester: keep-alive connections spread over processes, posting realistic events,
while an SSE listener times how long each alarm takes to come back.

    uv run python -m loadgen.load --rate 5000 --duration 30
    uv run python -m loadgen.load --max --duration 10

Each connection owns a fixed set of devices, so a device's events stay in order
like a real sensor's. In --rate mode latency counts from when a request was due,
so a server that falls behind shows it.
"""

import argparse
import asyncio
import contextlib
import multiprocessing
import random
import statistics
import time
from urllib.request import urlopen

import orjson
import uvloop

from service.events import now_ms, to_iso, to_ms

EVENT_WEIGHTS = {"heartbeat": 50, "motion": 15, "presence": 4,
                 "sleep_state": 3, "net_status": 3, "fall_warn": 1}
EVENT_TYPES, WEIGHTS = list(EVENT_WEIGHTS), list(EVENT_WEIGHTS.values())


def make_event(device, kind, ts):
    event = {"device_id": f"dev_{device:04d}", "room_id": f"room_{device // 2:03d}",
             "type": kind, "ts": to_iso(ts)}
    match kind:
        case "presence":
            event["in_room"] = random.random() < 0.5
        case "motion":
            event["magnitude"] = round(random.random(), 2)
        case "sleep_state":
            event["state"] = random.choice(["asleep", "awake", "unknown"])
        case "fall_warn":
            event["confidence"] = round(random.uniform(0.7, 0.99), 2)
        case "net_status":
            event["rssi"] = random.randint(-90, -50)
    return event


async def post(reader, writer, event):
    body = orjson.dumps(event)
    writer.write(b"POST /events HTTP/1.1\r\nHost: load\r\nContent-Type: application/json\r\n"
                 b"Content-Length: %d\r\n\r\n%s" % (len(body), body))
    head = await reader.readuntil(b"\r\n\r\n")
    await reader.readexactly(int(head.split(b"content-length: ")[1].split(b"\r\n")[0]))
    return head[9:12] == b"202"


async def connect(args):
    """Open a connection, waiting out a server restart."""
    while True:
        try:
            return await asyncio.open_connection(args.host, args.port)
        except ConnectionError:
            await asyncio.sleep(0.1)


async def connection(args, devices, interval, deadline, stats):
    reader, writer = await connect(args)
    due = time.perf_counter()
    while due < deadline:
        await asyncio.sleep(due - time.perf_counter())
        kind = random.choices(EVENT_TYPES, WEIGHTS)[0]
        event = make_event(random.choice(devices), kind, now_ms())
        copies = random.randint(1, 3) if kind == "fall_warn" else 1
        if kind == "fall_warn":
            stats["falls"] += 1
        for _ in range(copies):
            try:
                accepted = await post(reader, writer, event)
            except (ConnectionError, asyncio.IncompleteReadError):
                accepted = False
                reader, writer = await connect(args)
            stats["accepted" if accepted else "failed"] += 1
            stats["latencies"].append(time.perf_counter() - due)
            due = due + interval if interval else time.perf_counter()


def worker(args, first_connection, results):
    async def run():
        stats = {"accepted": 0, "failed": 0, "falls": 0, "latencies": []}
        deadline = time.perf_counter() + args.duration
        interval = 0 if args.max else args.connections / args.rate
        per_process = args.connections // args.processes
        await asyncio.gather(*(
            connection(args, range(c, args.devices, args.connections), interval, deadline, stats)
            for c in range(first_connection, first_connection + per_process)))
        stats["finished"] = time.time()
        results.put(stats)

    uvloop.run(run())


async def listen_for_alarms(args, latencies):
    reader, writer = await asyncio.open_connection(args.host, args.port)
    writer.write(b"GET /alarms/stream HTTP/1.1\r\nHost: load\r\n\r\n")
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(args.duration + 2):
            while line := await reader.readline():
                if line.startswith(b"data: "):
                    latencies.append(now_ms() - to_ms(orjson.loads(line[6:])["ts"]))


def percentiles(values):
    q = statistics.quantiles(values, n=100)
    return f"p50 {q[49]:.1f}ms  p95 {q[94]:.1f}ms  p99 {q[98]:.1f}ms"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=5000, help="events per second")
    parser.add_argument("--max", action="store_true", help="send as fast as the server accepts")
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--connections", type=int, default=200)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--devices", type=int, default=5000)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    started = now_ms()
    results = multiprocessing.Queue()
    per_process = args.connections // args.processes
    workers = [multiprocessing.Process(target=worker, args=(args, p * per_process, results))
               for p in range(args.processes)]
    for w in workers:
        w.start()
    alarm_latencies = []
    uvloop.run(listen_for_alarms(args, alarm_latencies))
    stats = [results.get() for _ in workers]
    for w in workers:
        w.join()

    elapsed = max(s["finished"] for s in stats) - started / 1000
    accepted = sum(s["accepted"] for s in stats)
    failed = sum(s["failed"] for s in stats)
    falls = sum(s["falls"] for s in stats)
    latencies = [l * 1000 for s in stats for l in s["latencies"]]
    alarms = orjson.loads(urlopen(f"http://{args.host}:{args.port}/alarms?since={started / 1000}").read())["alarms"]

    target = "max" if args.max else f"{args.rate:,}/s"
    print(f"accepted   {accepted:,} in {elapsed:.1f}s = {accepted / elapsed:,.0f}/s (target {target})")
    print(f"failed     {failed:,}")
    print(f"POST       {percentiles(latencies)}")
    print(f"alarms     {falls:,} distinct falls sent, {len(alarms):,} in /alarms")
    if len(alarm_latencies) > 1:
        print(f"alarm feed {percentiles(alarm_latencies)} from event ts")


if __name__ == "__main__":
    main()
