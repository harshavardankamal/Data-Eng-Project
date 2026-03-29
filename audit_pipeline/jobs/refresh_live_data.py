from __future__ import annotations

import argparse

from audit_pipeline.services.refresh_service import refresh_if_due
from audit_pipeline.settings import AUTO_REFRESH_MINUTES, DEFAULT_LIVE_LIMIT, DEFAULT_SAMPLE_STATIONS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scheduled refresh job for live telemetry snapshots.")
    parser.add_argument("--stations", nargs="+", default=DEFAULT_SAMPLE_STATIONS, help="Station IDs to fetch.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIVE_LIMIT, help="Recent observations per station.")
    parser.add_argument(
        "--max-age-minutes",
        type=int,
        default=AUTO_REFRESH_MINUTES,
        help="Refresh only if the newest snapshot is older than this many minutes. Default is 1440 (24 hours).",
    )
    parser.add_argument("--force", action="store_true", help="Refresh immediately even if the last snapshot is newer than the max age.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = refresh_if_due(
        args.stations,
        limit=args.limit,
        max_age_minutes=0 if args.force else args.max_age_minutes,
    )
    snapshot = result.get("snapshot")
    print(f"{result['status']}: {result['message']}")
    if snapshot:
        print(f"Latest snapshot: {snapshot['snapshot_id']}")


if __name__ == "__main__":
    main()
