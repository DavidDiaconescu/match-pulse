#!/usr/bin/env bash
# Creates the MatchPulse Kafka topics. Safe to run any number of times:
# --if-not-exists skips topics that are already there.
#
# Runs inside the kafka-init container (see docker-compose.yml), which already
# waits for the broker healthcheck. It can also be re-run by hand:
#   docker compose run --rm kafka-init
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

# Wait for the broker anyway, so the script also works when run on its own.
echo "Waiting for Kafka at ${BOOTSTRAP_SERVER} ..."
for attempt in $(seq 1 30); do
  if "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; then
    break
  fi
  if [ "${attempt}" -eq 30 ]; then
    echo "ERROR: Kafka not reachable after 30 attempts" >&2
    exit 1
  fi
  sleep 2
done

# create_topic <name> <partitions>
# Replication factor is 1 because the local cluster has a single broker.
create_topic() {
  "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
  echo "  topic ready: $1 ($2 partitions)"
}

# match-events: 6 partitions, keyed by match_id, so up to 6 consumers can share
#   the work and each match's events stay in order inside one partition.
# match-control: 1 partition, so every lifecycle signal of every match is in one
#   total order (low volume, so no need to parallelise).
# match-alerts: written by the streaming job; 3 partitions is plenty.
create_topic match-events 6
create_topic match-control 1
create_topic match-alerts 3

echo "Topics on the broker:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list
