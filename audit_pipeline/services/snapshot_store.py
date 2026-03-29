from __future__ import annotations

from io import BytesIO

import pandas as pd

from audit_pipeline.services.blob_store import download_bytes, upload_bytes
from audit_pipeline.services.catalog_store import save_catalog, save_latest_snapshot_metadata, snapshot_blob_name


def load_snapshot_dataframe(snapshot_id: str, layer: str) -> pd.DataFrame:
    blob_name = snapshot_blob_name(layer, snapshot_id)
    try:
        raw_bytes = download_bytes(blob_name)
    except FileNotFoundError:
        return pd.DataFrame()
    return pd.read_parquet(BytesIO(raw_bytes))


def persist_snapshot(layer: str, snapshot_id: str, df: pd.DataFrame) -> str:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    blob_name = snapshot_blob_name(layer, snapshot_id)
    upload_bytes(blob_name, buffer.getvalue(), content_type="application/octet-stream")
    return blob_name


def save_catalog_and_publish(entries: list[dict]) -> None:
    save_catalog(entries)
    if entries:
        save_latest_snapshot_metadata(entries[-1])
