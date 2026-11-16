"""MatchPulse replay producer: stream StatsBomb match events into Kafka at match speed.

Examples (from the repo root, with the stack running):
  python producer/replay.py --matches 3788741 --speed 60
  python producer/replay.py --matches producer/matches/demo.txt --speed 60
  docker compose run --rm replay-producer --matches 3788741 --speed 60

--speed 60 means one minute of match time per second; --speed 0 sends as fast as possible.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from loader import MatchData, load_match, parse_match_args
from schedule import DEFAULT_BREAKS, build_schedule
from transform import SCHEMA_VERSION, transform_event, utc_now_iso

EVENTS_TOPIC = "match-events"
CONTROL_TOPIC = "match-control"
PROGRESS_EVERY = 500  # log progress every N events per match

log = logging.getLogger("replay")


class ReplayStats:
    """Counters shared by all replay threads: events sent (per match) and delivery reports.

    Several threads update these at once, so every change happens under a lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events_sent = 0
        self.sent_per_match: dict[str, int] = {}
        self.total_per_match: dict[str, int] = {}
        self.delivered = 0
        self.failed = 0

    def start_match(self, match_id: str, total_events: int) -> None:
        with self._lock:
            self.sent_per_match[match_id] = 0
            self.total_per_match[match_id] = total_events

    def event_sent(self, match_id: str) -> None:
        with self._lock:
            self.events_sent += 1
            self.sent_per_match[match_id] += 1

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        """Called by the Kafka client once the broker confirms (or rejects) a message."""
        with self._lock:
            if err is None:
                self.delivered += 1
            else:
                self.failed += 1
                log.error("delivery failed for key %s on %s: %s", msg.key(), msg.topic(), err)

    def progress(self) -> str:
        with self._lock:
            return ", ".join(f"{m} {self.sent_per_match[m]}/{self.total_per_match[m]}"
                             for m in self.sent_per_match)


def report_throughput(stats: ReplayStats, interval: float, done: threading.Event) -> None:
    """Log events/second and totals every `interval` seconds until `done` is set."""
    start = last_time = time.monotonic()
    last_count = 0
    while not done.wait(interval):
        now, count = time.monotonic(), stats.events_sent
        rate = (count - last_count) / (now - last_time)
        log.info("throughput: %.0f events/s (last %.0fs) | total %d events, avg %.0f events/s | %s",
                 rate, now - last_time, count, count / (now - start), stats.progress())
        last_time, last_count = now, count


def build_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "matchpulse-replay",
        # Idempotence (implies acks=all): a retried send can neither duplicate nor
        # reorder messages, which keeps each match's events in order.
        "enable.idempotence": True,
        # Wait up to 5 ms to batch messages together: higher throughput, tiny delay.
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def send(producer: Producer, topic: str, key: str, value: dict[str, Any], stats: ReplayStats) -> None:
    payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=payload, on_delivery=stats.on_delivery)
            break
        except BufferError:
            # The local send queue is full: let librdkafka deliver some messages, then retry.
            producer.poll(0.5)
    producer.poll(0)  # trigger delivery callbacks for messages already acknowledged


def control_message(match: MatchData, replay_id: str, signal_name: str, period: int,
                    minute: int, second: int, events_emitted: int) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "match_id": match.match_id,
        "replay_id": replay_id,
        "signal": signal_name,
        "period": period,
        "minute": minute,
        "second": second,
        "home_team": match.home_team,
        "away_team": match.away_team,
        "events_emitted": events_emitted,
        "emitted_at": utc_now_iso(),
    }


def replay_match(
    producer: Producer,
    match: MatchData,
    speed: float,
    stop: threading.Event,
    stats: ReplayStats,
    breaks: dict[int, float],
) -> int:
    """Replay one match. Returns the number of events sent."""
    replay_id = str(uuid.uuid4())  # lets consumers tell two replays of the same match apart
    steps = build_schedule(match.events, breaks)
    total_events = sum(1 for s in steps if s.kind == "event")
    stats.start_match(match.match_id, total_events)
    log.info("match %s: %d events, replay_id %s, speed %sx", match.match_id, total_events, replay_id, speed)

    start = time.monotonic()
    match_elapsed = 0.0  # match-time seconds since the replay started
    sent = 0

    for step in steps:
        match_elapsed += step.wait_seconds
        if speed > 0:
            # Sleep until this step's absolute target time. Computing the target from
            # the start time (not "sleep the gap") avoids drift building up.
            delay = start + match_elapsed / speed - time.monotonic()
            if stop.wait(max(0.0, delay)):  # returns True as soon as Ctrl+C is pressed
                break
        elif stop.is_set():
            break

        if step.kind == "event":
            assert step.raw_event is not None
            send(producer, EVENTS_TOPIC, match.match_id,
                 transform_event(step.raw_event, match.match_id, replay_id), stats)
            stats.event_sent(match.match_id)
            sent += 1
            if sent % PROGRESS_EVERY == 0:
                log.info("match %s: %d/%d events sent (P%d %d')",
                         match.match_id, sent, total_events, step.period, step.minute)
        else:
            if step.signal == "full_time":
                # Every event is delivered before full_time goes out. (With several
                # matches this also waits for the other matches' queued messages, which
                # takes a few milliseconds.)
                producer.flush(30)
            assert step.signal is not None
            send(producer, CONTROL_TOPIC, match.match_id,
                 control_message(match, replay_id, step.signal, step.period,
                                 step.minute, step.second, sent), stats)
            log.info("match %s: %s (P%d %d:%02d)", match.match_id, step.signal,
                     step.period, step.minute, step.second)

    if stop.is_set():
        log.warning("match %s: stopped early after %d/%d events", match.match_id, sent, total_events)
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay StatsBomb matches into Kafka.")
    parser.add_argument("--matches", nargs="+", required=True,
                        help="match ids and/or files with one match id per line")
    parser.add_argument("--speed", type=float, default=60.0,
                        help="replay speed: 1 = real time, 60 = 1 match minute per second, 0 = no waiting (default 60)")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"),
                        help="Kafka bootstrap servers (default $KAFKA_BOOTSTRAP or localhost:9094)")
    parser.add_argument("--skip-breaks", action="store_true",
                        help="don't wait during half-time and other breaks between periods")
    parser.add_argument("--report-every", type=float, default=5.0,
                        help="seconds between throughput log lines (default 5)")
    args = parser.parse_args(argv)
    if args.speed < 0:
        parser.error("--speed must be >= 0")
    return args


def replay_in_thread(producer: Producer, match_id: str, args: argparse.Namespace, stop: threading.Event,
                     stats: ReplayStats, breaks: dict[int, float], errors: list[str]) -> None:
    """Thread body: one thread replays one match. Errors are collected, not raised,
    so one broken match doesn't stop the others."""
    try:
        replay_match(producer, load_match(match_id), args.speed, stop, stats, breaks)
    except Exception as exc:  # noqa: BLE001 - report any failure of this match
        log.exception("match %s failed", match_id)
        errors.append(f"{match_id}: {exc}")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    args = parse_args(argv)
    try:
        match_ids = parse_match_args(args.matches)
    except ValueError as exc:
        log.error("bad --matches: %s", exc)
        return 2
    breaks = {} if args.skip_breaks else DEFAULT_BREAKS

    # Ctrl+C (SIGINT) and `docker stop` (SIGTERM) set this flag; every wait checks it.
    stop = threading.Event()
    def request_stop(signum: int, _frame: Any) -> None:
        log.warning("received %s, stopping after the current message...", signal.Signals(signum).name)
        stop.set()
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    # One producer is shared by all threads (the Kafka client is thread-safe).
    producer = build_producer(args.bootstrap)
    stats = ReplayStats()
    errors: list[str] = []
    log.info("connecting to Kafka at %s, matches: %s", args.bootstrap, ", ".join(match_ids))

    # One thread per match, so all matches play at the same time, like a real match day.
    threads = [threading.Thread(target=replay_in_thread, name=f"match-{m}",
                                args=(producer, m, args, stop, stats, breaks, errors))
               for m in match_ids]
    reporting_done = threading.Event()
    reporter = threading.Thread(target=report_throughput, args=(stats, args.report_every, reporting_done), daemon=True)

    start = time.monotonic()
    reporter.start()
    for thread in threads:
        thread.start()
    # join() with a timeout in a loop keeps the main thread able to receive Ctrl+C.
    for thread in threads:
        while thread.is_alive():
            thread.join(0.5)
    reporting_done.set()

    remaining = producer.flush(30)
    elapsed = time.monotonic() - start
    log.info("done: %d matches, %d events sent in %.1fs (avg %.0f events/s), "
             "%d messages delivered, %d failed, %d undelivered",
             len(match_ids), stats.events_sent, elapsed, stats.events_sent / max(elapsed, 1e-9),
             stats.delivered, stats.failed, remaining)
    for error in errors:
        log.error("failed match: %s", error)
    return 0 if stats.failed == 0 and remaining == 0 and not errors else 1


if __name__ == "__main__":
    sys.exit(main())
