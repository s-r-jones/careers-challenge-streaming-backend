"""
Plain ASGI on uvicorn. WORKERS processes share the port; each appends to the
event log and builds its state by following it. One of them snapshots every minute.

    WORKERS=4 uv run python -m service.main
"""

import asyncio
import fcntl
import os
import pickle
import threading
import time
from pathlib import Path
from urllib.parse import parse_qsl

import orjson
import uvicorn

from service import events
from service.alarms import Alarms, render
from service.event_log import EventLog
from service.health import Health
from service.metrics import Metrics
from service.occupancy import Occupancy

health = Health()
occupancy = Occupancy()
alarms = Alarms()
metrics = Metrics()
event_log = EventLog(Path("data/events.jsonl"))
SNAPSHOT = Path("data/snapshot.pickle")


def apply(event, now):
    match event.type:
        case "heartbeat":
            health.add(event, now)
        case "presence":
            occupancy.add(event, now)
        case "fall_warn":
            alarms.add(event, now)


def catch_up():
    """Apply what any process appended to the log since the last call."""
    now = events.now_ms()
    count = 0
    for arrival, event in event_log.read_new():
        apply(event, now)
        metrics.applied(event, now - arrival)
        count += 1
    return count


async def follow_log():
    while True:
        catch_up()
        await asyncio.sleep(0.005)  # well inside the 1s alarm budget


def save_snapshot():
    """State and the log position it covers. Renamed into place, so a kill mid-save keeps the last one."""
    # pickle can't handle the defaultdict lambdas
    state = (event_log.reader.tell(), health.last_heartbeat, dict(health.seconds),
             dict(occupancy.changes), alarms.by_id)
    temp = SNAPSHOT.with_suffix(f".{os.getpid()}.tmp")
    temp.write_bytes(pickle.dumps(state))
    os.replace(temp, SNAPSHOT)


def load_snapshot():
    position, last_heartbeat, seconds, changes, by_id = pickle.loads(SNAPSHOT.read_bytes())
    health.last_heartbeat.update(last_heartbeat)
    health.seconds.update(seconds)
    occupancy.changes.update(changes)
    alarms.by_id.update(by_id)
    event_log.reader.seek(position)


async def save_snapshots():
    """Every minute, from whichever worker gets the lock."""
    lock = open(SNAPSHOT.with_suffix(".lock"), "w")
    while True:
        await asyncio.sleep(60)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            continue
        save_snapshot()


async def log_metrics():
    applied = metrics.events.total()
    while True:
        await asyncio.sleep(10)
        rate = (metrics.events.total() - applied) / 10
        applied = metrics.events.total()
        print(orjson.dumps({"events_per_s": rate} | metrics_report()).decode(), flush=True)


def metrics_report():
    return metrics.report() | {"devices": len(health.last_heartbeat), "rooms": len(occupancy.changes),
                               "alarms_held": len(alarms.by_id), "subscribers": len(alarms.subscribers)}


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        await lifespan(receive, send)
        return
    now = events.now_ms()
    query = dict(parse_qsl(scope["query_string"].decode()))
    match scope["method"], scope["path"].strip("/").split("/"):
        case "POST", ["events"]:
            await post_event(receive, send, now)
        case "GET", ["devices", device_id, "health"]:
            await respond(send, 200, health.for_device(device_id, now))
        case "GET", ["rooms", room_id, "occupancy"]:
            await respond(send, 200, occupancy.for_room(room_id, query.get("window", "5m"), now))
        case "GET", ["alarms"]:
            since = query.get("since", "0")
            await respond(send, 200, {"alarms": alarms.since(since), "since": since})
        case "GET", ["alarms", "stream"]:
            await stream_alarms(scope, receive, send)
        case "GET", ["metrics"]:
            await respond(send, 200, metrics_report())
        case _:
            await respond(send, 404, {"error": "not found"})


async def lifespan(receive, send):
    await receive()  # startup
    threading.Thread(target=exit_with_parent, daemon=True).start()
    started = time.perf_counter()
    from_snapshot = SNAPSHOT.exists()
    if from_snapshot:
        load_snapshot()
    replayed = catch_up()
    metrics.event_delays.clear()  # replayed events arrived long ago
    metrics.alarm_delays.clear()
    print(orjson.dumps({"worker": os.getpid(), "from_snapshot": from_snapshot, "replayed": replayed,
                        "seconds": round(time.perf_counter() - started, 2)}).decode(), flush=True)
    tasks = [asyncio.create_task(follow_log()), asyncio.create_task(save_snapshots()),
             asyncio.create_task(log_metrics())]
    await send({"type": "lifespan.startup.complete"})
    await receive()  # shutdown
    for task in tasks:
        task.cancel()
    await send({"type": "lifespan.shutdown.complete"})


async def post_event(receive, send, now):
    try:
        event = events.parse(orjson.loads(await read_body(receive)), now)
    except events.INVALID as error:
        metrics.rejected[repr(error)] += 1
        await respond(send, 422, {"error": repr(error)})
        return
    event_log.append(event, now)
    metrics.received += 1
    await respond(send, 202, {"ok": True})


async def stream_alarms(scope, receive, send):
    """Server-sent events. Last-Event-ID resumes where the client left off."""
    queue = asyncio.Queue()
    last_id = dict(scope["headers"]).get(b"last-event-id")
    # no await between the backlog and subscribing, so nothing is missed
    if last_id:
        for event in alarms.after(last_id.decode()):
            queue.put_nowait(event)
    alarms.subscribers.add(queue)
    # uvicorn doesn't raise on disconnect, so watch receive() for it
    watcher = asyncio.create_task(end_on_disconnect(receive, queue))

    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"text/event-stream"), (b"cache-control", b"no-cache")]})
    while (event := await queue.get()) is not None:
        alarm = render(event)
        message = f"id: {alarm['event_id']}\ndata: ".encode() + orjson.dumps(alarm) + b"\n\n"
        await send({"type": "http.response.body", "body": message, "more_body": True})
    alarms.subscribers.discard(queue)
    await watcher


async def end_on_disconnect(receive, queue):
    while (await receive())["type"] != "http.disconnect":
        pass
    queue.put_nowait(None)


def exit_with_parent():
    """Die with the parent instead of holding the port."""
    parent = os.getppid()
    while os.getppid() == parent:
        time.sleep(0.1)
    os._exit(1)


async def read_body(receive) -> bytes:
    body, more = b"", True
    while more:
        message = await receive()
        body += message.get("body", b"")
        more = message.get("more_body", False)
    return body


async def respond(send, status, body):
    payload = orjson.dumps(body)
    await send({"type": "http.response.start", "status": status,
                "headers": [(b"content-type", b"application/json"),
                            (b"content-length", str(len(payload)).encode())]})
    await send({"type": "http.response.body", "body": payload})


if __name__ == "__main__":
    threading.Thread(target=exit_with_parent, daemon=True).start()
    # "::" listens on v4 and v6
    uvicorn.run("service.main:app", host="::", port=8080, workers=int(os.environ.get("WORKERS", 4)),
                loop="uvloop", http="httptools", access_log=False)
