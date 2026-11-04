from service.alarms import Alarms
from service.events import HOUR_MS, Event, to_ms

NOW = to_ms("2026-10-29T12:00:00.000Z")


def fall(ts, device="dev_0001", confidence=0.9):
    return Event(device, "room_000", "fall_warn", ts, confidence)


def test_jitter_copies_become_one_alarm():
    alarms = Alarms()
    for _ in range(3):
        alarms.add(fall(NOW), NOW)
    assert alarms.since("0") == [{"event_id": "dev_0001:" + str(NOW), "device_id": "dev_0001", "room_id": "room_000",
                                  "ts": "2026-10-29T12:00:00.000Z", "confidence": 0.9}]


def test_distinct_falls_stay_distinct():
    alarms = Alarms()
    alarms.add(fall(NOW), NOW)
    alarms.add(fall(NOW + 1), NOW)
    alarms.add(fall(NOW, device="dev_0002"), NOW)
    assert len(alarms.since("0")) == 3


def test_since_takes_iso_or_epoch_seconds():
    alarms = Alarms()
    alarms.add(fall(NOW - 1000), NOW)
    alarms.add(fall(NOW), NOW)
    assert len(alarms.since("2026-10-29T12:00:00.000Z")) == 1
    assert len(alarms.since(str(NOW / 1000))) == 1


def test_after_returns_what_a_reconnecting_client_missed():
    alarms = Alarms()
    for ts in (NOW, NOW + 1, NOW + 2):
        alarms.add(fall(ts), NOW)
    assert [e.ts for e in alarms.after(f"dev_0001:{NOW}")] == [NOW + 1, NOW + 2]
    assert len(alarms.after("unknown")) == 3


def test_alarms_older_than_an_hour_are_forgotten():
    alarms = Alarms()
    alarms.add(fall(NOW - HOUR_MS - 1), NOW)
    alarms.add(fall(NOW), NOW)
    assert [a["ts"] for a in alarms.since("0")] == ["2026-10-29T12:00:00.000Z"]
