"""
One JSON line per accepted event, [arrival_ms, *event], appended before the ack.
Every process follows the same file, so they all see events in the same order.
O_APPEND keeps small writes whole. No fsync: a process kill loses nothing, only
a power cut would.
"""

import orjson

from service.events import Event


class EventLog:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = open(path, "ab", buffering=0)
        self.reader = open(path, "rb")

    def append(self, event, now):
        self.writer.write(orjson.dumps([now, *event]) + b"\n")

    def read_new(self):
        """(arrival, event) pairs appended since the last call, by any process."""
        while chunk := self.reader.read(1_000_000):
            end = chunk.rfind(b"\n") + 1
            self.reader.seek(end - len(chunk), 1)  # leave a half-written line for next time
            if not end:
                break
            for line in chunk[:end].splitlines():
                arrival, *fields = orjson.loads(line)
                yield arrival, Event(*fields)
