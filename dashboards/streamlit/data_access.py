"""The ONLY place that knows where the gold tables live and how to read them.

If Mihai changes a table path or the bucket, change it here (or via environment
variables); the pages never build paths themselves.

Tables are Delta Lake tables read with the `deltalake` package (delta-rs): no Spark.
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

# Base location of the gold layer. Examples:
#   s3://lake/gold          MinIO (default, see docs/gold-contract.md)
#   /data/gold-local        a local folder (handy for quick experiments)
GOLD_URI = os.getenv("GOLD_URI", "s3://lake/gold").rstrip("/")

# Table name -> sub-path under GOLD_URI. Change the right-hand side if paths change.
TABLES: dict[str, str] = {
    "team_season_stats": "team_season_stats",
    "player_season_stats": "player_season_stats",
    "league_trends": "league_trends",
    "shots": "shots",
}

# dev_fixtures.py writes this prefix into the table description, so the app can warn
# that it is showing sample data instead of Mihai's real gold tables.
DEV_MARKER = "DEV FIXTURE"


def table_uri(name: str) -> str:
    return f"{GOLD_URI}/{TABLES[name]}"


def storage_options() -> dict[str, str]:
    """Connection settings for delta-rs. Only needed for s3:// paths (MinIO)."""
    if not GOLD_URI.startswith("s3://"):
        return {}
    return {
        "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("S3_REGION", "us-east-1"),  # MinIO ignores it, delta-rs needs one
        "AWS_ALLOW_HTTP": "true",                            # local MinIO has no TLS
    }


class TableUnavailable(Exception):
    """Raised when a gold table cannot be opened (missing, MinIO down, ...)."""


# Cache results for 5 minutes, so switching pages doesn't re-download the tables.
@st.cache_data(ttl=300, show_spinner="Loading gold table...")
def read_table(name: str) -> pd.DataFrame:
    try:
        return DeltaTable(table_uri(name), storage_options=storage_options()).to_pandas()
    except Exception as exc:  # delta-rs raises different errors for missing table / no connection
        raise TableUnavailable(f"{table_uri(name)}: {exc}") from exc


@st.cache_data(ttl=300)
def table_description(name: str) -> str | None:
    """Description stored in the Delta table metadata (None if missing or unreadable)."""
    try:
        return DeltaTable(table_uri(name), storage_options=storage_options()).metadata().description
    except Exception:
        return None


def is_dev_data(name: str) -> bool:
    return (table_description(name) or "").startswith(DEV_MARKER)
