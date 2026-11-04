from service.event_log import EventLog
from service.events import Event, to_ms

NOW = to_ms("2026-10-29T12:00:00.000Z")


def event(ts):
    return Event("dev_0001", "room_000", "presence", ts, True)


def read_events(log):
    return [e for _, e in log.read_new()]


def test_read_new_returns_each_event_once(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    log.append(event(NOW), NOW)
    assert read_events(log) == [event(NOW)]
    log.append(event(NOW + 1), NOW)
    assert read_events(log) == [event(NOW + 1)]
    assert read_events(log) == []


def test_lines_carry_their_arrival_time(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    log.append(event(NOW - 500), NOW)
    assert list(log.read_new()) == [(NOW, event(NOW - 500))]


def test_processes_see_the_same_events_in_the_same_order(tmp_path):
    a = EventLog(tmp_path / "events.jsonl")
    b = EventLog(tmp_path / "events.jsonl")
    for i in range(6):
        (a if i % 2 else b).append(event(NOW + i), NOW)
    assert read_events(a) == read_events(b) == [event(NOW + i) for i in range(6)]


def test_restart_replays_everything(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    for i in range(3):
        log.append(event(NOW + i), NOW)
    assert read_events(EventLog(tmp_path / "events.jsonl")) == [event(NOW + i) for i in range(3)]


def test_half_written_line_waits_for_the_rest(tmp_path):
    path = tmp_path / "events.jsonl"
    log = EventLog(path)
    log.append(event(NOW), NOW)
    with open(path, "ab") as file:
        file.write(b'[%d,"dev_0001","room_000",' % NOW)
    assert read_events(log) == [event(NOW)]
    with open(path, "ab") as file:
        file.write(b'"presence",%d,true]\n' % (NOW + 1))
    assert read_events(log) == [event(NOW + 1)]
