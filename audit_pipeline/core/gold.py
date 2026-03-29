from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd


def transform_gold(silver_df: pd.DataFrame) -> pd.DataFrame:
    if silver_df.empty:
        return pd.DataFrame(
            columns=[
                "partition_date",
                "station_id",
                "station_name",
                "rows_processed",
                "new_records",
                "anomaly_count",
                "avg_temperature_c",
                "avg_wind_speed_kmh",
                "max_wind_gust_kmh",
                "avg_pressure_pa",
                "first_seen",
                "last_seen",
                "updated_at",
            ]
        )

    gold = (
        silver_df.groupby(["observation_date", "station_id", "station_name"], dropna=False)
        .agg(
            rows_processed=("source_record_id", "count"),
            anomaly_count=("anomaly_flag", "sum"),
            avg_temperature_c=("temperature_c", "mean"),
            avg_wind_speed_kmh=("wind_speed_kmh", "mean"),
            max_wind_gust_kmh=("wind_gust_kmh", "max"),
            avg_pressure_pa=("pressure_pa", "mean"),
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
        )
        .reset_index()
        .rename(columns={"observation_date": "partition_date"})
    )
    gold["new_records"] = 0
    gold["updated_at"] = datetime.now(UTC).isoformat()
    return gold
