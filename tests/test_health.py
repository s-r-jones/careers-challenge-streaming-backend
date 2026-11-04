from service.events import Event, to_ms
from service.health import Health

NOW = to_ms("2026-10-29T12:00:00.000Z")


def heartbeat(seconds_ago):
    return Event("dev_0001", "room_000", "heartbeat", NOW - seconds_ago * 1000, None)


def test_availability_counts_seconds_with_a_heartbeat():
    health = Health()
    for seconds_ago in range(0, 300, 2):
        health.add(heartbeat(seconds_ago), NOW)
    assert health.for_device("dev_0001", NOW)["availability_5m"] == 0.5


def test_heartbeats_older_than_five_minutes_dont_count():
    health = Health()
    health.add(heartbeat(301), NOW)
    assert health.for_device("dev_0001", NOW)["availability_5m"] == 0
    assert len(health.seconds["dev_0001"]) == 0


def test_repeated_heartbeat_counts_once():
    health = Health()
    health.add(heartbeat(10), NOW)
    health.add(heartbeat(10), NOW)
    assert health.for_device("dev_0001", NOW)["availability_5m"] == 0.0033


def test_late_heartbeat_fills_history_but_not_latest():
    health = Health()
    health.add(heartbeat(0), NOW)
    health.add(heartbeat(100), NOW)
    report = health.for_device("dev_0001", NOW)
    assert report["availability_5m"] == 0.0067
    assert report["last_heartbeat_ts"] == "2026-10-29T12:00:00.000Z"


def test_clock_running_ahead_keeps_older_heartbeats():
    health = Health()
    health.add(heartbeat(280), NOW)
    health.add(heartbeat(-20), NOW)  # device clock 20s ahead of ours
    assert health.for_device("dev_0001", NOW)["availability_5m"] == 0.0033


def test_device_with_no_heartbeat_yet():
    assert Health().for_device("dev_9999", NOW) == {"last_heartbeat_ts": None, "availability_5m": 0.0}
