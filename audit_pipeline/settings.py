from __future__ import annotations

import os
from typing import Final

OFFICIAL_ARCHIVE_ROOT = "https://api.weather.gov"

DEFAULT_LIVE_LIMIT: Final[int] = 72
AUTO_REFRESH_MINUTES: Final[int] = int(os.getenv("AUTO_REFRESH_MINUTES", "1440"))
SNAPSHOT_CACHE_TTL_SECONDS: Final[int] = int(os.getenv("SNAPSHOT_CACHE_TTL_SECONDS", "600"))
DEFAULT_SAMPLE_STATIONS: Final[list[str]] = ["KJFK", "KSEA", "KSFO"]
STATION_OPTIONS: Final[dict[str, str]] = {
    "KJFK": "New York, Kennedy International Airport",
    "KSEA": "Seattle-Tacoma International Airport",
    "KSFO": "San Francisco International Airport",
    "KORD": "Chicago O'Hare International Airport",
    "KATL": "Atlanta Hartsfield-Jackson International Airport",
}
QUERY_OPTIONS: Final[list[str]] = [
    "Show only new records",
    "Compare snapshot changes",
    "Aggregate by partition",
]
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
AZURE_BLOB_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "telemetry-lakehouse").strip() or "telemetry-lakehouse"


def nws_headers() -> dict[str, str]:
    return {
        "User-Agent": os.getenv(
            "NWS_USER_AGENT",
            "(open-table-format-time-travel-audit-pipeline, contact@example.com)",
        ),
        "Accept": "application/geo+json",
    }
