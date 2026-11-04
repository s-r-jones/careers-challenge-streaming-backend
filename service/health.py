"""Per device: the latest heartbeat, and which of the last 300 seconds had one."""

from array import array
from bisect import bisect_left
from collections import defaultdict

from service.events import to_iso

WINDOW_S = 300


class Health:
    def __init__(self):
        self.last_heartbeat = {}  # device_id -> ts of its newest heartbeat
        # device_id -> seconds with a heartbeat, sorted. array('q') keeps the snapshot small.
        self.seconds = defaultdict(lambda: array("q"))

    def add(self, event, now):
        if event.ts > self.last_heartbeat.get(event.device_id, 0):
            self.last_heartbeat[event.device_id] = event.ts
        seconds = self.seconds[event.device_id]
        second = event.ts // 1000
        i = bisect_left(seconds, second)
        is_new = i == len(seconds) or seconds[i] != second
        if is_new:
            seconds.insert(i, second)
        window_start = now // 1000 - WINDOW_S
        while seconds and seconds[0] <= window_start:
            del seconds[0]

    def for_device(self, device_id, now):
        now_second = now // 1000
        seconds = self.seconds.get(device_id, [])
        recent = [second for second in seconds if now_second - WINDOW_S < second <= now_second]
        last = self.last_heartbeat.get(device_id)
        return {"last_heartbeat_ts": to_iso(last) if last else None,
                "availability_5m": round(len(recent) / WINDOW_S, 4)}
