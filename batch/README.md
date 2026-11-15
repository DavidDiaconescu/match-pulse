# batch/

**Owner:** Mihai

Batch layer of the Lambda architecture: Spark jobs that load historical data into a
Delta Lake on MinIO using a bronze / silver / gold layout.

- Sources: StatsBomb Open Data JSON and the Kaggle "European Soccer Database" (SQLite).
- Output: gold tables consumed by the Streamlit app. The proposed schemas are in
  [`docs/gold-contract.md`](../docs/gold-contract.md) (to be agreed).

Expected entry points (called by `make load-history` and `make run-batch`):
- `batch/load_history.sh`: ingest raw sources into bronze.
- `batch/run_batch.sh`: build silver and gold.

Status: placeholder. Mihai owns this folder.
