# streaming/

**Owner:** Dayana

Speed layer of the Lambda architecture: Spark Structured Streaming jobs that consume
`match-events` and `match-control` from Kafka, compute live xG, momentum and anomaly
alerts, write live state to Redis and alerts to the `match-alerts` topic.

- Input message format: [`docs/event-schema.md`](../docs/event-schema.md)
- Inside Docker the broker is `kafka:9092`; from the host it is `localhost:9094`.

Status: placeholder. Dayana owns this folder.
