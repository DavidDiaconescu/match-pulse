"""Plan a match replay: in which order to send things, and how long to wait before each.

The plan is a list of Steps. A step is either a raw event to send to `match-events` or a
lifecycle signal to send to `match-control`. Each step carries how many seconds of
*match time* to wait before sending it; replay.py divides that by --speed.

Keeping this a pure function (no Kafka, no sleeping) makes it easy to unit-test.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from transform import clock_to_seconds

# Real-world break (in match seconds) before each period starts, keyed by that period.
# 2: half-time, 3: before extra time, 4: extra-time half-time, 5: before penalties.
DEFAULT_BREAKS: dict[int, float] = {2: 15 * 60, 3: 5 * 60, 4: 60, 5: 5 * 60}


@dataclass(frozen=True)
class Step:
    kind: str                      # "event" or "control"
    wait_seconds: float            # match-time seconds to wait before this step
    period: int
    minute: int
    second: int
    raw_event: dict[str, Any] | None = None   # set when kind == "event"
    signal: str | None = None                 # set when kind == "control"


def sort_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order events by (period, index).

    `index` is StatsBomb's own sequence number. We deliberately do NOT sort by timestamp:
    the source data contains occasional wrong timestamps (e.g. a Ball Receipt at 45:53
    recorded as 00:00:00.189), and sorting by timestamp would move such events to the
    wrong place. See docs/event-schema.md section 5.
    """
    return sorted(events, key=lambda e: (e["period"], e["index"]))


def build_schedule(
    events: list[dict[str, Any]],
    breaks: dict[int, float] | None = None,
) -> list[Step]:
    """Return the full replay plan for one match: control signals + events with waits."""
    breaks = DEFAULT_BREAKS if breaks is None else breaks
    ordered = sort_events(events)
    if not ordered:
        return []

    periods = sorted({e["period"] for e in ordered})
    last_period = periods[-1]
    steps: list[Step] = []

    for period in periods:
        period_events = [e for e in ordered if e["period"] == period]
        first, last = period_events[0], period_events[-1]

        # --- signal at the start of the period -------------------------------
        signal = "kick_off" if period == periods[0] else "second_half_start"
        wait = 0.0 if period == periods[0] else breaks.get(period, 0.0)
        steps.append(Step("control", wait, period, first["minute"], first["second"], signal=signal))

        # --- the events, each waiting for the gap since the previous one -----
        # `latest_clock` is the furthest point in the period clock seen so far. Using
        # it (instead of the previous event's clock) means one event with a wrong,
        # too-early timestamp causes neither a negative wait nor a huge wait after it.
        latest_clock = 0.0
        for raw in period_events:
            clock = clock_to_seconds(raw["timestamp"])
            gap = max(0.0, clock - latest_clock)
            latest_clock = max(latest_clock, clock)
            steps.append(Step("event", gap, period, raw["minute"], raw["second"], raw_event=raw))

        # --- signal at the end of the period ---------------------------------
        signal = "full_time" if period == last_period else "half_time"
        steps.append(Step("control", 0.0, period, last["minute"], last["second"], signal=signal))

    return steps
