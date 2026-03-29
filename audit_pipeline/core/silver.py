from __future__ import annotations

import pandas as pd


def transform_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    silver = bronze_df.copy()
    silver["observation_date"] = silver["timestamp"].dt.date.astype("string")
    silver["observation_hour"] = silver["timestamp"].dt.floor("h")
    silver["quality_flag"] = silver.apply(
        lambda row: "PASS"
        if pd.notna(row["temperature_c"]) and pd.notna(row["pressure_pa"]) and pd.notna(row["humidity_pct"])
        else "REVIEW",
        axis=1,
    )
    silver["anomaly_flag"] = (
        silver["wind_gust_kmh"].fillna(0).gt(70)
        | silver["pressure_pa"].fillna(101325).lt(98000)
        | silver["temperature_c"].fillna(0).gt(38)
        | silver["temperature_c"].fillna(0).lt(-25)
    )
    silver["incremental_partition"] = silver["observation_date"] + "|" + silver["station_id"].astype("string")
    silver = silver.drop_duplicates(subset=["source_record_id"]).reset_index(drop=True)
    return silver

