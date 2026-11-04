import asyncio

from service import main
from service.events import Event, now_ms


def fall(ts):
    return Event("dev_0001", "room_000", "fall_warn", ts, 0.9)


async def open_stream(headers=()):
    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "http.request", "body": b"", "more_body": False})
    sent = []

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": "GET", "path": "/alarms/stream",
             "query_string": b"", "headers": list(headers)}
    task = asyncio.create_task(main.app(scope, incoming.get, send))
    await asyncio.sleep(0.01)
    return task, incoming, sent


def bodies(sent):
    return b"".join(m.get("body", b"") for m in sent[1:])


def test_stream_pushes_new_alarms_and_ends_on_disconnect():
    async def scenario():
        task, incoming, sent = await open_stream()
        now = now_ms()
        main.alarms.add(fall(now), now)
        main.alarms.add(fall(now), now)  # jitter copy
        await asyncio.sleep(0.01)
        assert bodies(sent).count(b"data: ") == 1
        assert f"id: dev_0001:{now}\n".encode() in bodies(sent)

        incoming.put_nowait({"type": "http.disconnect"})
        await asyncio.wait_for(task, 1)
        assert not main.alarms.subscribers

    asyncio.run(scenario())


def test_reconnect_with_last_event_id_resumes():
    async def scenario():
        now = now_ms() + 10_000
        for ts in (now, now + 1, now + 2):
            main.alarms.add(fall(ts), now)
        task, incoming, sent = await open_stream([(b"last-event-id", f"dev_0001:{now}".encode())])
        assert bodies(sent).count(b"data: ") == 2
        incoming.put_nowait({"type": "http.disconnect"})
        await asyncio.wait_for(task, 1)

    asyncio.run(scenario())
