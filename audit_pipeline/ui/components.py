from __future__ import annotations

import pandas as pd
import streamlit as st

from audit_pipeline.services.query_service import run_snapshot_query
from audit_pipeline.services.catalog_store import format_duration_ms, snapshot_label
from audit_pipeline.settings import QUERY_OPTIONS

ARCHITECTURE_TEXT = {
    "NOAA/NWS API": "Official live source for daily telemetry-like weather observations.",
    "Raw Blob": "The landed source pull stored unchanged in Azure Blob Storage for auditability and replay.",
    "Bronze Blob": "Raw observations with ingest metadata and partition keys, stored as versioned parquet in Blob Storage.",
    "Silver Blob": "Cleaned and quality-checked observations with anomaly flags and stable query columns.",
    "Gold Blob": "Station and date level aggregates used for analytics and snapshot comparison.",
    "DuckDB Compute": "Stateless in-memory SQL used by the app to compare snapshots and run interactive queries.",
    "Streamlit Explorer": "Read-only browser UI that reads published snapshots and metadata without ingesting data itself.",
}


def apply_custom_css() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
        .layer-card {
            border: 1px solid rgba(26, 82, 118, 0.18);
            border-radius: 18px;
            padding: 1rem 1rem 0.75rem 1rem;
            background: linear-gradient(180deg, #f7fbfd 0%, #eef6f7 100%);
            min-height: 118px;
        }
        .layer-title {
            font-weight: 700;
            color: #10485d;
            margin-bottom: 0.35rem;
        }
        .layer-value {
            font-size: 1.6rem;
            font-weight: 800;
            color: #0b6e4f;
        }
        .layer-caption {
            color: #4f6b75;
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_architecture_strip() -> None:
    st.subheader("Architecture Strip")
    if "selected_architecture" not in st.session_state:
        st.session_state["selected_architecture"] = "Bronze Blob"

    labels = list(ARCHITECTURE_TEXT.keys())
    cols = st.columns(len(labels))
    for col, label in zip(cols, labels):
        with col:
            if st.button(label, use_container_width=True):
                st.session_state["selected_architecture"] = label

    st.info(ARCHITECTURE_TEXT[st.session_state["selected_architecture"]])


def render_snapshot_slider(catalog: list[dict]) -> str:
    st.subheader("Time-Travel Slider")
    label_map = {snapshot_label(item["snapshot_id"], item["created_at"]): item["snapshot_id"] for item in catalog}
    labels = list(label_map.keys())
    default_label = labels[-2] if len(labels) > 1 else labels[0]
    chosen_label = st.select_slider("Pick a historical snapshot", options=labels, value=default_label)
    st.caption(f"Current snapshot is always the latest: `{catalog[-1]['snapshot_id']}`")
    return label_map[chosen_label]


def render_query_panel(layer: str, previous_df: pd.DataFrame, current_df: pd.DataFrame) -> None:
    st.subheader("Query Panel")
    button_cols = st.columns(len(QUERY_OPTIONS))
    chosen_query = st.session_state.get("query_choice", QUERY_OPTIONS[0])
    for col, label in zip(button_cols, QUERY_OPTIONS):
        with col:
            if st.button(label, use_container_width=True):
                st.session_state["query_choice"] = label
                chosen_query = label

    custom_sql = st.text_area(
        "Optional SQL editor",
        value="SELECT * FROM current_data ORDER BY 1 LIMIT 25",
        height=120,
    )
    sql, result = run_snapshot_query(layer, previous_df, current_df, chosen_query, custom_sql)
    st.code(sql, language="sql")
    st.dataframe(result, use_container_width=True, height=320)


def render_metrics_panel(latest_snapshot: dict) -> None:
    st.subheader("Metrics Panel")
    metrics = latest_snapshot["metrics"]
    cols = st.columns(5)
    cols[0].metric("Rows Processed", f"{metrics['rows_processed']:,}")
    cols[1].metric("Partitions Scanned", metrics["partitions_scanned_incremental"])
    cols[2].metric("Full Scan Rows", f"{metrics['full_scan_rows']:,}")
    cols[3].metric("Incremental Scan Rows", f"{metrics['incremental_scan_rows']:,}")
    cols[4].metric("Time Saved", f"{metrics['time_saved_pct']}%")
    st.caption(f"Latest processing duration: {format_duration_ms(metrics['processing_time_ms'])}")
    st.caption(f"Latest snapshot timestamp: {latest_snapshot['created_at']}")

    scan_df = pd.DataFrame(
        {
            "mode": ["Full scan", "Incremental"],
            "rows": [metrics["full_scan_rows"], metrics["incremental_scan_rows"]],
        }
    ).set_index("mode")
    st.bar_chart(scan_df)


def render_bsg_flow(latest_snapshot: dict) -> None:
    counts = latest_snapshot["row_counts"]
    cols = st.columns(3)
    cards = [
        ("Bronze", counts["bronze"], "Raw landed observations"),
        ("Silver", counts["silver"], "Normalized, quality-checked records"),
        ("Gold", counts["gold"], "Partition aggregates for audit/querying"),
    ]
    for col, (title, value, caption) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div class="layer-card">
                    <div class="layer-title">{title}</div>
                    <div class="layer-value">{value:,}</div>
                    <div class="layer-caption">{caption}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
