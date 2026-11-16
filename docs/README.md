# docs/

**Owner:** shared

Contracts between the three parts of the project. Change these only after agreeing
with the people affected.

| Document | What it defines | Producers | Consumers |
|---|---|---|---|
| [`event-schema.md`](event-schema.md) | Kafka message formats for `match-events` and `match-control` | David (replay producer) | Dayana (streaming) |
| [`schemas/`](schemas/) | Machine-readable JSON Schemas for the messages above | | |
| [`gold-contract.md`](gold-contract.md) | Gold Delta tables (PROPOSAL) | Mihai (batch) | David (Streamlit) |
| [`demo-script.md`](demo-script.md) | 10-minute demo walkthrough | David | presenters |
| [`DOCUMENTATIE_RO.md`](DOCUMENTATIE_RO.md) | Romanian summary of all 4 phases, how it works, next steps | David | team, professors |
