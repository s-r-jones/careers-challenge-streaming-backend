from service import main
from service.alarms import Alarms
from service.events import Event, now_ms
from service.health import Health
from service.occupancy import Occupancy


def test_restart_from_a_snapshot_gives_the_same_answers(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "SNAPSHOT", tmp_path / "snapshot.pickle")
    now = now_ms()
    main.health.add(Event("dev_0001", "room_000", "heartbeat", now, None), now)
    main.occupancy.add(Event("dev_0001", "room_000", "presence", now - 30_000, True), now)
    main.alarms.add(Event("dev_0001", "room_000", "fall_warn", now, 0.9), now)

    def answers():
        return (main.health.for_device("dev_0001", now), main.occupancy.for_room("room_000", "1m", now),
                main.alarms.since("0"))

    before = answers()
    main.save_snapshot()
    for name, fresh in (("health", Health()), ("occupancy", Occupancy()), ("alarms", Alarms())):
        monkeypatch.setattr(main, name, fresh)
    main.load_snapshot()
    assert answers() == before
