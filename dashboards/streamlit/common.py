"""Small UI helpers shared by all pages."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from data_access import TableUnavailable, is_dev_data, read_table


def load(name: str) -> pd.DataFrame:
    """Read a gold table; if it is unavailable, show a helpful message and stop the page."""
    try:
        df = read_table(name)
    except TableUnavailable as exc:
        st.error(f"Gold table **{name}** is not available.\n\n`{exc}`")
        st.info("No gold data yet? Build the sample tables with `make dev-fixtures`, "
                "or run Mihai's batch job with `make run-batch`.")
        st.stop()
    if is_dev_data(name):
        st.warning(f"**DEV DATA**: `{name}` contains sample tables built by `dev_fixtures.py` "
                   "from StatsBomb Open Data, not the real gold layer.", icon="🧪")
    return df


def select(label: str, options: list, key: str, index: int = 0):
    """A sidebar selectbox that stops the page politely when there is nothing to choose."""
    if not options:
        st.info(f"No {label.lower()} available for this selection.")
        st.stop()
    return st.sidebar.selectbox(label, options, index=min(index, len(options) - 1), key=key)


def fmt(value, digits: int = 0, missing: str = "–") -> str:
    """Format a number for a metric card; show a dash for missing values."""
    if value is None or pd.isna(value):
        return missing
    return f"{value:.{digits}f}"
