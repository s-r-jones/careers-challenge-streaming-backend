"""Per room: presence changes from all its devices, sorted by ts so late events land in place."""

from array import array
from bisect import bisect_right
from collections import defaultdict

from service.events import HOUR_MS

WINDOWS = {"1m": 60_000, "5m": 300_000, "1h": HOUR_MS}


class Occupancy:
    def __init__(self):
        # room_id -> (ts of each change, in_room after it), sorted by ts. array() keeps the snapshot small.
        self.changes = defaultdict(lambda: (array("q"), array("b")))

    def add(self, event, now):
        times, in_room = self.changes[event.room_id]
        i = bisect_right(times, event.ts)
        times.insert(i, event.ts)
        in_room.insert(i, event.value)
        # drop changes older than the hour, keeping the last one before it as the starting state
        hour_start = now - HOUR_MS
        while len(times) > 1 and times[1] <= hour_start:
            del times[0]
            del in_room[0]

    def for_room(self, room_id, window, now):
        times, in_room = self.changes.get(room_id, ([], []))
        window_ms = WINDOWS[window]
        window_start = now - window_ms
        occupied_ms = 0
        for i, ts in enumerate(times):
            if in_room[i]:
                # occupied from this change to the next (or now), clipped to the window
                next_ts = times[i + 1] if i + 1 < len(times) else now
                occupied_from = max(ts, window_start)
                occupied_until = min(next_ts, now)
                occupied_ms += max(0, occupied_until - occupied_from)
        return {"in_room": bool(in_room[-1]) if times else False,
                "occupied_pct": round(occupied_ms / window_ms, 4), "window_seconds": window_ms // 1000}
