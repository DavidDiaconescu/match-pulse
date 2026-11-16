#!/usr/bin/env bash
# (Re)create the Kafka topics (safe to repeat) and show their partitions.
source "$(dirname "$0")/_common.sh"
docker compose run --rm kafka-init > /dev/null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe \
  | grep -E "^Topic:" | cut -f1,3,4
