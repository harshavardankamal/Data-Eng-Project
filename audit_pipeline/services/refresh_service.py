from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from audit_pipeline.core.bronze import transform_bronze
from audit_pipeline.core.gold import transform_gold
from audit_pipeline.core.silver import transform_silver
from audit_pipeline.services.catalog_store import load_catalog, next_snapshot_id
from audit_pipeline.services.snapshot_store import (
    load_snapshot_dataframe,
    persist_snapshot,
    save_catalog_and_publish,
)
from audit_pipeline.services.source_nws import cache_raw_download, fetch_recent_observations
from audit_pipeline.settings import AUTO_REFRESH_MINUTES, DEFAULT_LIVE_LIMIT, DEFAULT_SAMPLE_STATIONS


def build_metrics(
    bronze_df: pd.DataFrame,
    incremental_df: pd.DataFrame,
    started_at: float,
    previous_snapshot_id: str | None,
) -> dict[str, Any]:
    full_scan_rows = int(len(bronze_df))
    incremental_rows = int(len(incremental_df)) if previous_snapshot_id else full_scan_rows
    full_partitions = int(bronze_df["partition_date"].nunique()) if not bronze_df.empty else 0
    incremental_partitions = (
        int(incremental_df["timestamp"].dt.date.nunique()) if previous_snapshot_id and not incremental_df.empty else full_partitions
    )
    time_saved_pct = 0.0
    if full_scan_rows:
        time_saved_pct = max(0.0, (1 - (incremental_rows / full_scan_rows)) * 100)
    return {
        "rows_processed": incremental_rows,
        "partitions_scanned_incremental": incremental_partitions,
        "partitions_scanned_full": full_partitions,
        "full_scan_rows": full_scan_rows,
        "incremental_scan_rows": incremental_rows,
        "processing_time_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "time_saved_pct": round(time_saved_pct, 2),
    }


def create_snapshot_from_dataframe(
    raw_df: pd.DataFrame,
    station_ids: list[str],
    source_name: str,
    catalog: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    catalog = load_catalog() if catalog is None else catalog
    started_at = time.perf_counter()
    previous_snapshot_id = catalog[-1]["snapshot_id"] if catalog else None
    previous_bronze = load_snapshot_dataframe(previous_snapshot_id, "bronze") if previous_snapshot_id else pd.DataFrame()
    raw_path = cache_raw_download(raw_df)

    bronze_df = transform_bronze(raw_df, source_name=source_name)
    if not previous_bronze.empty:
        merged = pd.concat([previous_bronze, bronze_df], ignore_index=True)
        bronze_df = merged.drop_duplicates(subset=["source_record_id"]).sort_values("timestamp").reset_index(drop=True)
        incremental_df = bronze_df[~bronze_df["source_record_id"].isin(previous_bronze["source_record_id"])]
    else:
        incremental_df = bronze_df.copy()

    silver_df = transform_silver(bronze_df)
    gold_df = transform_gold(silver_df)
    if not gold_df.empty:
        partition_counts = (
            incremental_df.assign(observation_date=incremental_df["timestamp"].dt.date.astype("string"))
            .groupby(["observation_date", "station_id"])["source_record_id"]
            .count()
            .reset_index(name="new_records")
        )
        gold_df = gold_df.merge(
            partition_counts.rename(columns={"observation_date": "partition_date"}),
            on=["partition_date", "station_id"],
            how="left",
            suffixes=("", "_latest"),
        )
        gold_df["new_records"] = gold_df["new_records_latest"].fillna(0).astype(int)
        gold_df = gold_df.drop(columns=["new_records_latest"])

    snapshot_id = next_snapshot_id(catalog)
    entry = {
        "snapshot_id": snapshot_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_name": source_name,
        "station_ids": station_ids,
        "raw_path": raw_path,
        "paths": {
            "bronze": persist_snapshot("bronze", snapshot_id, bronze_df),
            "silver": persist_snapshot("silver", snapshot_id, silver_df),
            "gold": persist_snapshot("gold", snapshot_id, gold_df),
        },
        "row_counts": {"bronze": int(len(bronze_df)), "silver": int(len(silver_df)), "gold": int(len(gold_df))},
        "metrics": build_metrics(bronze_df, incremental_df, started_at, previous_snapshot_id),
    }
    catalog.append(entry)
    save_catalog_and_publish(catalog)
    return entry


def refresh_live_snapshot(station_ids: list[str], limit: int = DEFAULT_LIVE_LIMIT) -> dict[str, Any]:
    catalog = load_catalog()

    raw_df = fetch_recent_observations(station_ids, limit=limit)
    if raw_df.empty:
        return {"status": "SKIPPED", "snapshot": catalog[-1] if catalog else None, "message": "No data returned from source"}
    latest_bronze = load_snapshot_dataframe(catalog[-1]["snapshot_id"], "bronze") if catalog else pd.DataFrame()
    if not latest_bronze.empty and set(raw_df["source_record_id"]).issubset(set(latest_bronze["source_record_id"])):
        return {"status": "NO_CHANGE", "snapshot": catalog[-1], "message": "No new source records"}
    snapshot = create_snapshot_from_dataframe(raw_df, station_ids=station_ids, source_name="noaa_nws_live", catalog=catalog)
    return {"status": "CREATED", "snapshot": snapshot, "message": "Snapshot created"}


def refresh_if_due(
    station_ids: list[str] | None = None,
    limit: int = DEFAULT_LIVE_LIMIT,
    max_age_minutes: int = AUTO_REFRESH_MINUTES,
) -> dict[str, Any]:
    station_ids = station_ids or DEFAULT_SAMPLE_STATIONS
    catalog = load_catalog()
    if not catalog:
        return refresh_live_snapshot(station_ids, limit=limit)

    latest = catalog[-1]
    latest_at = datetime.fromisoformat(latest["created_at"])
    if datetime.now(UTC) - latest_at >= timedelta(minutes=max_age_minutes):
        return refresh_live_snapshot(station_ids, limit=limit)
    return {"status": "SKIPPED_NOT_DUE", "snapshot": latest, "message": "Latest snapshot is less than 24 hours old"}
