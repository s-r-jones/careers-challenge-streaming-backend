"""Fall warnings, one per (device_id, ts): sensor jitter repeats a fall with the same ts."""

from service.events import HOUR_MS, to_iso, to_ms


class Alarms:
    def __init__(self):
        self.by_id = {}           # alarm id -> Event, in arrival order
        self.subscribers = set()  # one asyncio.Queue per open stream

    def add(self, event, now):
        key = alarm_id(event)
        if key not in self.by_id:
            self.by_id[key] = event
            for queue in self.subscribers:
                queue.put_nowait(event)
        while self.by_id:
            oldest_id = next(iter(self.by_id))
            if self.by_id[oldest_id].ts >= now - HOUR_MS:
                break
            del self.by_id[oldest_id]

    def since(self, since: str):
        """`since` is an ISO timestamp, or epoch seconds like the example stub takes."""
        since_ms = to_ms(since) if "T" in since else float(since) * 1000
        return [render(event) for event in self.by_id.values() if event.ts >= since_ms]

    def after(self, last_id):
        """Alarms that arrived after `last_id`; all we have if we don't know it."""
        keys = list(self.by_id)
        start = keys.index(last_id) + 1 if last_id in self.by_id else 0
        return [self.by_id[key] for key in keys[start:]]


def alarm_id(event):
    return f"{event.device_id}:{event.ts}"


def render(event):
    return {"event_id": alarm_id(event), "device_id": event.device_id, "room_id": event.room_id,
            "ts": to_iso(event.ts), "confidence": event.value}
