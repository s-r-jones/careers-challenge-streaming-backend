"""Counters behind GET /metrics and the periodic log line."""

import os
import statistics
from collections import Counter, deque


class Metrics:
    def __init__(self):
        self.received = 0                         # events this worker accepted
        self.rejected = Counter()                 # by reason, this worker only
        self.events = Counter()                   # by type, from the log: same in every worker
        self.event_delays = deque(maxlen=10_000)  # ms from arrival to applied, recent events
        self.alarm_delays = deque(maxlen=1_000)   # same for fall warnings: when they hit the feed

    def applied(self, event, delay):
        self.events[event.type] += 1
        self.event_delays.append(delay)
        if event.type == "fall_warn":
            self.alarm_delays.append(delay)

    def report(self):
        return {"worker": os.getpid(), "received": self.received, "rejected": dict(self.rejected),
                "events": dict(self.events),
                "delay_ms": {"events": percentiles(self.event_delays),
                             "alarms": percentiles(self.alarm_delays)}}


def percentiles(delays):
    if len(delays) < 2:
        return None
    cuts = statistics.quantiles(delays, n=20)
    return {"p50": cuts[9], "p95": cuts[18]}
