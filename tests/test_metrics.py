from service.events import Event
from service.metrics import Metrics


def event(kind):
    return Event("dev_0001", "room_000", kind, 0, None)


def test_counts_by_type_and_times_alarms_separately():
    metrics = Metrics()
    for delay in range(1, 101):
        metrics.applied(event("heartbeat"), delay)
    metrics.applied(event("fall_warn"), 7)
    metrics.applied(event("fall_warn"), 9)
    report = metrics.report()
    assert report["events"] == {"heartbeat": 100, "fall_warn": 2}
    assert report["delay_ms"]["events"]["p95"] > 90
    assert report["delay_ms"]["alarms"]["p50"] == 8


def test_no_percentiles_until_there_is_data():
    assert Metrics().report()["delay_ms"] == {"events": None, "alarms": None}
