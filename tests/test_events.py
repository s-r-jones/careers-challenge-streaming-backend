import pytest

from service import events

NOW = events.to_ms("2026-10-29T12:00:00.000Z")


def raw(**fields):
    return {"device_id": "dev_0001", "room_id": "room_000", "type": "heartbeat",
            "ts": "2026-10-29T12:00:00.000Z"} | fields


def test_ts_is_exact_to_the_millisecond():
    assert events.to_ms("2026-10-29T12:00:00.123Z") - NOW == 123
    assert events.to_ms("2026-10-29T13:00:00.123+01:00") - NOW == 123


def test_window_is_one_hour_each_way():
    events.parse(raw(ts="2026-10-29T11:00:00.000Z"), NOW)
    events.parse(raw(ts="2026-10-29T13:00:00.000Z"), NOW)
    for ts in ["2026-10-29T10:59:59.999Z", "2026-10-29T13:00:00.001Z"]:
        with pytest.raises(ValueError):
            events.parse(raw(ts=ts), NOW)


def test_value_is_the_type_specific_field():
    assert events.parse(raw(type="fall_warn", confidence=0.92), NOW).value == 0.92
    assert events.parse(raw(type="presence", in_room=False), NOW).value is False
    assert events.parse(raw(), NOW).value is None


@pytest.mark.parametrize("bad", [
    raw(type="teleport"),
    raw(type="presence"),
    raw(type="presence", in_room="yes"),
    raw(type="fall_warn", confidence="high"),
    raw(ts="yesterday"),
    {k: v for k, v in raw().items() if k != "room_id"},
    [1],
])
def test_malformed_is_invalid(bad):
    with pytest.raises(events.INVALID):
        events.parse(bad, NOW)
