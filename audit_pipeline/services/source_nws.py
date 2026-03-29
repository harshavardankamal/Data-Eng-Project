from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

import pandas as pd
import requests

from audit_pipeline.services.blob_store import upload_bytes
from audit_pipeline.services.catalog_store import raw_blob_name, safe_float
from audit_pipeline.settings import DEFAULT_LIVE_LIMIT, OFFICIAL_ARCHIVE_ROOT, STATION_OPTIONS, nws_headers


@dataclass(frozen=True)
class StationTarget:
    station_id: str
    station_name: str


DEFAULT_TARGETS = [
    StationTarget(station_id=station_id, station_name=station_name)
    for station_id, station_name in STATION_OPTIONS.items()
]


def _extract_value(payload: dict[str, Any], field_name: str) -> float | None:
    return safe_float(payload.get(field_name, {}).get("value"))


def fetch_station_observations(station_id: str, limit: int = DEFAULT_LIVE_LIMIT) -> list[dict[str, Any]]:
    response = requests.get(
        f"{OFFICIAL_ARCHIVE_ROOT}/stations/{station_id}/observations",
        params={"limit": limit},
        headers=nws_headers(),
        timeout=30,
    )
    response.raise_for_status()
    features = response.json().get("features", [])

    records: list[dict[str, Any]] = []
    fetched_at = datetime.now(UTC).isoformat()
    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates") or [None, None]
        records.append(
            {
                "source_record_id": properties.get("@id"),
                "station_id": properties.get("stationId", station_id),
                "station_name": properties.get("stationName", STATION_OPTIONS.get(station_id, station_id)),
                "timestamp": properties.get("timestamp"),
                "text_description": properties.get("textDescription"),
                "temperature_c": _extract_value(properties, "temperature"),
                "dewpoint_c": _extract_value(properties, "dewpoint"),
                "wind_direction_deg": _extract_value(properties, "windDirection"),
                "wind_speed_kmh": _extract_value(properties, "windSpeed"),
                "wind_gust_kmh": _extract_value(properties, "windGust"),
                "pressure_pa": _extract_value(properties, "barometricPressure"),
                "visibility_m": _extract_value(properties, "visibility"),
                "humidity_pct": _extract_value(properties, "relativeHumidity"),
                "elevation_m": _extract_value(properties, "elevation"),
                "latitude": safe_float(coordinates[1]),
                "longitude": safe_float(coordinates[0]),
                "raw_message": properties.get("rawMessage"),
                "icon": properties.get("icon"),
                "present_weather_count": len(properties.get("presentWeather", [])),
                "fetched_at": fetched_at,
                "source_url": f"{OFFICIAL_ARCHIVE_ROOT}/stations/{station_id}/observations",
                "raw_payload_json": json.dumps(feature),
            }
        )
    return records


def fetch_recent_observations(station_ids: list[str], limit: int = DEFAULT_LIVE_LIMIT) -> pd.DataFrame:
    all_records: list[dict[str, Any]] = []
    for station_id in station_ids:
        all_records.extend(fetch_station_observations(station_id, limit=limit))
    df = pd.DataFrame(all_records)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["source_record_id", "timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


def cache_raw_download(df: pd.DataFrame) -> str:
    created_at = datetime.now(UTC).isoformat()
    blob_name = raw_blob_name("noaa_nws_live", created_at)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    upload_bytes(blob_name, buffer.getvalue(), content_type="application/octet-stream")
    return blob_name
