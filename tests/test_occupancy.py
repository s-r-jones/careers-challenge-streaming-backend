from service.events import Event, to_ms
from service.occupancy import Occupancy

NOW = to_ms("2026-10-29T12:00:00.000Z")


def presence(seconds_ago, in_room, device="dev_0001"):
    return Event(device, "room_000", "presence", NOW - seconds_ago * 1000, in_room)


def test_occupied_half_of_the_last_minute():
    occupancy = Occupancy()
    occupancy.add(presence(60, True), NOW)
    occupancy.add(presence(30, False), NOW)
    assert occupancy.for_room("room_000", "1m", NOW) == {"in_room": False, "occupied_pct": 0.5,
                                                         "window_seconds": 60}


def test_late_event_fixes_history():
    occupancy = Occupancy()
    occupancy.add(presence(60, True), NOW)
    occupancy.add(presence(30, False), NOW)
    occupancy.add(presence(15, True, device="dev_0002"), NOW)  # arrives last, happened 15s ago
    assert occupancy.for_room("room_000", "1m", NOW)["in_room"] is True
    assert occupancy.for_room("room_000", "1m", NOW)["occupied_pct"] == 0.75


def test_state_from_before_the_hour_carries_in():
    occupancy = Occupancy()
    occupancy.add(presence(3 * 3600, False), NOW)
    occupancy.add(presence(2 * 3600, True), NOW)
    times, _ = occupancy.changes["room_000"]
    assert len(times) == 1
    assert occupancy.for_room("room_000", "1h", NOW)["occupied_pct"] == 1.0


def test_no_change_known_before_first_event_counts_as_empty():
    occupancy = Occupancy()
    occupancy.add(presence(30, True), NOW)
    assert occupancy.for_room("room_000", "1m", NOW)["occupied_pct"] == 0.5
    assert occupancy.for_room("room_000", "1h", NOW)["occupied_pct"] == 0.0083


def test_room_with_no_presence_yet_is_empty():
    assert Occupancy().for_room("room_999", "5m", NOW) == {"in_room": False, "occupied_pct": 0.0,
                                                           "window_seconds": 300}
