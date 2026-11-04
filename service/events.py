"""Event parsing and the ±1h acceptance window."""

import time
from datetime import UTC, datetime
from typing import NamedTuple

HOUR_MS = 3_600_000

# missing field, unknown type, bad ts
INVALID = (KeyError, TypeError, ValueError)

NUMBER = (int, float)

# extra field per event type and its JSON type; heartbeats have none
VALUE_FIELD = {
    "presence": ("in_room", bool),
    "motion": ("magnitude", NUMBER),
    "sleep_state": ("state", str),
    "fall_warn": ("confidence", NUMBER),
    "net_status": ("rssi", NUMBER),
}


class Event(NamedTuple):
    device_id: str
    room_id: str
    type: str
    ts: int  # device clock, ms since epoch
    value: object


def parse(raw: dict, now: int) -> Event:
    ts = to_ms(raw["ts"])
    if not now - HOUR_MS <= ts <= now + HOUR_MS:
        raise ValueError("ts more than 1h from server time")
    value = None
    if raw["type"] != "heartbeat":
        field, kind = VALUE_FIELD[raw["type"]]
        value = raw[field]
        if not isinstance(value, kind):
            raise ValueError(f"{field} has the wrong type")
    return Event(raw["device_id"], raw["room_id"], raw["type"], ts, value)


def to_ms(iso: str) -> int:
    return round(datetime.fromisoformat(iso).timestamp() * 1000)


def to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_ms() -> int:
    return time.time_ns() // 1_000_000
