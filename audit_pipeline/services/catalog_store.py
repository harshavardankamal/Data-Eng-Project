from __future__ import annotations

from datetime import datetime
from typing import Any

from audit_pipeline.services.blob_store import download_json, upload_json

CATALOG_BLOB = "metadata/catalog.json"
LATEST_SNAPSHOT_BLOB = "metadata/latest_snapshot.json"


def load_catalog() -> list[dict[str, Any]]:
    return download_json(CATALOG_BLOB, default=[])


def save_catalog(entries: list[dict[str, Any]]) -> None:
    upload_json(CATALOG_BLOB, entries)


def load_latest_snapshot_metadata() -> dict[str, Any] | None:
    return download_json(LATEST_SNAPSHOT_BLOB, default=None)


def save_latest_snapshot_metadata(entry: dict[str, Any]) -> None:
    payload = {
        "latest_snapshot_id": entry["snapshot_id"],
        "snapshot_created_at": entry["created_at"],
        "source_name": entry["source_name"],
        "station_ids": entry["station_ids"],
        "row_counts": entry["row_counts"],
        "metrics": entry["metrics"],
        "raw_path": entry["raw_path"],
        "paths": entry["paths"],
    }
    upload_json(LATEST_SNAPSHOT_BLOB, payload)


def next_snapshot_id(entries: list[dict[str, Any]]) -> str:
    return f"T{len(entries) + 1}"


def snapshot_blob_name(layer: str, snapshot_id: str) -> str:
    return f"snapshots/{layer}/{snapshot_id}.parquet"


def raw_blob_name(source_name: str, created_at: str) -> str:
    timestamp = datetime.fromisoformat(created_at).strftime("%Y%m%dT%H%M%SZ")
    return f"raw/{source_name}/{timestamp}.parquet"


def snapshot_label(snapshot_id: str, created_at: str) -> str:
    timestamp = datetime.fromisoformat(created_at)
    return f"{snapshot_id} ({timestamp.strftime('%Y-%m-%d %H:%M')})"


def format_duration_ms(value: float) -> str:
    if value < 1000:
        return f"{value:.0f} ms"
    return f"{value / 1000:.2f} s"


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
