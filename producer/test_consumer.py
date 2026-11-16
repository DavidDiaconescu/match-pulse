"""Check that a replayed match arrived in Kafka completely and in order.

Run it after (or during) a replay of the same match:
  python producer/test_consumer.py --match 3788741
  docker compose run --rm --entrypoint python replay-producer test_consumer.py --match 3788741

It checks the latest COMPLETED replay of the match (identified by replay_id), so older
or interrupted replays still sitting in the topic don't break the counts. Add --wait to
start it while a replay is running. Prints a PASS/FAIL report and exits with code 0
(pass) or 1 (fail).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from loader import events_path
from schedule import sort_events
from replay import CONTROL_TOPIC, EVENTS_TOPIC
from transform import utc_now_iso

SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", Path(__file__).resolve().parent.parent / "docs" / "schemas"))


def read_topic(bootstrap: str, topic: str, match_id: str) -> list[dict[str, Any]]:
    """Read every message of `topic` from the beginning up to its current end,
    and return the values whose key is `match_id`, in partition order."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"matchpulse-test-{uuid.uuid4()}",  # throwaway group, commits nothing
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        partitions = list(metadata.topics[topic].partitions)
        # Where does each partition end right now? We stop reading there.
        end_offsets = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
        to_read = {p for p, end in end_offsets.items() if end > 0}
        consumer.assign([TopicPartition(topic, p, 0) for p in partitions])  # offset 0 = beginning

        values: list[dict[str, Any]] = []
        while to_read:
            msg = consumer.poll(5.0)
            if msg is None:
                raise TimeoutError(f"no messages from {topic} for 5 s while partitions {sorted(to_read)} unfinished")
            if msg.error():
                raise RuntimeError(msg.error())
            if msg.key() == match_id.encode():
                values.append(json.loads(msg.value()))
            if msg.offset() >= end_offsets[msg.partition()] - 1:
                to_read.discard(msg.partition())
        return values
    finally:
        consumer.close()


def find_replay(bootstrap: str, match_id: str, wait: bool, timeout: float) -> list[dict[str, Any]]:
    """Return the control messages of the replay to check.

    Default: the latest replay that reached full_time (interrupted replays are skipped).
    With wait=True: the latest replay that started, waiting until its full_time arrives,
    so the test can run while the replay is still in progress. Replays that had already
    finished before this test started are ignored, so an old replay never passes for a
    new one that hasn't sent its kick_off yet.
    """
    started_at = utc_now_iso()  # same ISO format as emitted_at, so strings compare correctly
    deadline = time.monotonic() + timeout
    while True:
        controls = read_topic(bootstrap, CONTROL_TOPIC, match_id)
        finished = {c["replay_id"] for c in controls if c["signal"] == "full_time"
                    and (not wait or c["emitted_at"] >= started_at)}
        started = [c["replay_id"] for c in controls if c["signal"] == "kick_off"]
        candidates = started[-1:] if wait else [r for r in started if r in finished]
        if candidates and candidates[-1] in finished:
            return [c for c in controls if c["replay_id"] == candidates[-1]]
        if not wait:
            raise LookupError(f"no completed replay of match {match_id} in {CONTROL_TOPIC} "
                              "(use --wait while a replay is running)")
        if time.monotonic() > deadline:
            raise TimeoutError(f"replay of match {match_id} did not reach full_time within {timeout:.0f} s")
        print(f"waiting for full_time of match {match_id} ...")
        time.sleep(5)


def schema_errors(messages: list[dict[str, Any]]) -> list[str]:
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        return []
    validator = Draft202012Validator(json.loads((SCHEMA_DIR / "match-event.schema.json").read_text()))
    errors = []
    for message in messages:
        for error in validator.iter_errors(message):
            errors.append(f"index {message.get('index')}: {error.message}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify a replayed match in Kafka.")
    parser.add_argument("--match", required=True, help="match id to verify")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"))
    parser.add_argument("--file", type=Path, help="events JSON (default data/statsbomb/events/<match>.json)")
    parser.add_argument("--wait", action="store_true",
                        help="check the replay that is running now, waiting for its full_time")
    parser.add_argument("--timeout", type=float, default=600, help="seconds to wait with --wait (default 600)")
    args = parser.parse_args()

    events_file = args.file or events_path(args.match)
    if not events_file.exists():
        print(f"RESULT: FAIL ({events_file} not found; replay the match first, it downloads the file)")
        return 1
    raw_events = json.loads(events_file.read_text(encoding="utf-8"))
    expected_ids = [e["id"] for e in sort_events(raw_events)]

    try:
        controls = find_replay(args.bootstrap, args.match, args.wait, args.timeout)
    except (LookupError, TimeoutError) as exc:
        print(f"RESULT: FAIL ({exc})")
        return 1
    replay_id = controls[0]["replay_id"]
    received = [m for m in read_topic(args.bootstrap, EVENTS_TOPIC, args.match) if m.get("replay_id") == replay_id]
    received_ids = [m["event_id"] for m in received]
    full_time = next(c for c in controls if c["signal"] == "full_time")
    signals = [c["signal"] for c in controls]
    errors = schema_errors(received)

    checks = [
        ("event count equals JSON file", len(received) == len(expected_ids),
         f"received {len(received)}, file has {len(expected_ids)}"),
        ("no duplicate events", len(set(received_ids)) == len(received_ids),
         f"{len(received_ids) - len(set(received_ids))} duplicates"),
        ("same events as JSON file", set(received_ids) == set(expected_ids),
         f"{len(set(expected_ids) - set(received_ids))} missing, {len(set(received_ids) - set(expected_ids))} unexpected"),
        ("order preserved (period, index)", received_ids == expected_ids,
         "order matches" if received_ids == expected_ids else "order differs"),
        ("full_time.events_emitted matches", full_time["events_emitted"] == len(received),
         f"full_time says {full_time['events_emitted']}"),
        ("control signals in order", signals[0] == "kick_off" and signals[-1] == "full_time"
         and signals.count("half_time") == signals.count("second_half_start"),
         " -> ".join(signals)),
        ("messages match JSON Schema", not errors, f"{len(errors)} errors" + (f", first: {errors[0]}" if errors else "")),
    ]

    print(f"\nTest report: match {args.match}, replay {replay_id}")
    for name, ok, detail in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:<34} {detail}")
    passed = all(ok for _, ok, _ in checks)
    print(f"\nRESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
