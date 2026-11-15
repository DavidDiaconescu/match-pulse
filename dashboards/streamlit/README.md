# dashboards/streamlit/

**Owner:** David (person 1)

Streamlit app for historical analytics. It reads the gold Delta tables from MinIO
with the `deltalake` Python package (delta-rs), with no Spark involved.

Pages (planned):
1. Team profile by season
2. Player page with a shot map
3. League trends (home vs away, goals per season and league)

Expected tables: see [`docs/gold-contract.md`](../../docs/gold-contract.md).

Status: built in Phases 3–4.
