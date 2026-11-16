import random

from schedule import build_schedule, sort_events


def test_sort_by_period_then_index(mini_match):
    shuffled = mini_match[:]
    random.Random(42).shuffle(shuffled)
    assert [e["index"] for e in sort_events(shuffled)] == list(range(1, 20))


def test_sort_ignores_wrong_timestamps(mini_match):
    # Event 8 has timestamp 00:00:00.100 but belongs after the shot (index 7).
    order = [e["index"] for e in sort_events(mini_match)]
    assert order.index(8) == order.index(7) + 1


def test_control_signals_wrap_each_period(mini_match):
    steps = build_schedule(mini_match)
    controls = [(s.signal, s.period, s.minute, s.second) for s in steps if s.kind == "control"]
    assert controls == [
        ("kick_off", 1, 0, 0),
        ("half_time", 1, 45, 30),
        ("second_half_start", 2, 45, 0),
        ("full_time", 2, 92, 0),
    ]
    assert steps[0].signal == "kick_off" and steps[-1].signal == "full_time"


def test_every_event_appears_once_in_order(mini_match):
    events = [s.raw_event["index"] for s in build_schedule(mini_match) if s.kind == "event"]
    assert events == list(range(1, 20))


def test_waits_follow_the_clock_gaps(mini_match):
    waits = {s.raw_event["index"]: s.wait_seconds for s in build_schedule(mini_match) if s.kind == "event"}
    assert waits[5] == 1.0                 # 00:00:00 -> 00:00:01
    assert waits[6] == 1.5                 # 00:00:01 -> 00:00:02.5
    assert waits[7] == 597.5               # 00:00:02.5 -> 00:10:00


def test_wrong_timestamp_causes_no_negative_or_huge_wait(mini_match):
    waits = {s.raw_event["index"]: s.wait_seconds for s in build_schedule(mini_match) if s.kind == "event"}
    assert waits[8] == 0.0                 # 00:00:00.100 is earlier than 00:10:00: send immediately
    assert waits[9] == 600.0               # measured from 00:10:00, not from the wrong 00:00:00.100


def test_total_match_time_with_and_without_breaks(mini_match):
    with_break = sum(s.wait_seconds for s in build_schedule(mini_match))
    no_break = sum(s.wait_seconds for s in build_schedule(mini_match, breaks={}))
    assert no_break == 2730.0 + 2820.0     # 45:30 + 47:00 of playing time
    assert with_break - no_break == 900.0  # 15-minute half-time


def test_extra_time_signals():
    def ev(index, period, ts="00:00:00.000"):
        return {"index": index, "period": period, "timestamp": ts, "minute": 0, "second": 0}
    events = [ev(1, 1), ev(2, 2), ev(3, 3), ev(4, 4), ev(5, 5)]
    signals = [s.signal for s in build_schedule(events) if s.kind == "control"]
    assert signals == ["kick_off", "half_time",
                       "second_half_start", "half_time",
                       "second_half_start", "half_time",
                       "second_half_start", "half_time",
                       "second_half_start", "full_time"]


def test_empty_match():
    assert build_schedule([]) == []
