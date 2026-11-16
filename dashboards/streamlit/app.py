"""MatchPulse historical analytics: entry page.

Run: streamlit run app.py  (pages live in pages/, Streamlit adds them to the sidebar)
"""
import streamlit as st

from data_access import GOLD_URI, TABLES, TableUnavailable, is_dev_data, read_table, table_uri

st.set_page_config(page_title="MatchPulse", page_icon="⚽", layout="wide")

st.title("⚽ MatchPulse: historical analytics")
st.markdown(
    "Batch side of the MatchPulse Lambda architecture. This app reads the **gold** Delta "
    "tables produced by the Spark batch layer, straight from MinIO with *delta-rs* "
    "(no Spark needed).\n\n"
    "**Pages** (sidebar):\n"
    "1. **Team profile**: one team's season: results, home vs away, xG, league table\n"
    "2. **Player shot map**: every shot of a player on a pitch, sized by xG\n"
    "3. **League trends**: goals per match and home advantage across seasons and leagues"
)

st.subheader("Gold tables")
st.caption(f"Source: `{GOLD_URI}`")
rows = []
for name in TABLES:
    try:
        df = read_table(name)
        status = "🧪 dev data" if is_dev_data(name) else "✅ gold"
        rows.append({"table": name, "status": status, "rows": len(df), "path": table_uri(name)})
    except TableUnavailable:
        rows.append({"table": name, "status": "❌ missing", "rows": 0, "path": table_uri(name)})
st.dataframe(rows, hide_index=True, width="stretch")

if any(r["status"] == "❌ missing" for r in rows):
    st.info("Some tables are missing. For sample data run `make dev-fixtures`; "
            "the real tables come from Mihai's batch job (`make run-batch`).")
