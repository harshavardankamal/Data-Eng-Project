from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd


def transform_bronze(raw_df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    bronze = raw_df.copy()
    bronze["source_name"] = source_name
    bronze["ingest_ts"] = datetime.now(UTC).isoformat()
    bronze["partition_date"] = bronze["timestamp"].dt.date.astype("string")
    bronze["partition_station"] = bronze["station_id"].astype("string")
    bronze = bronze.sort_values(["timestamp", "station_id", "source_record_id"]).reset_index(drop=True)
    return bronze

