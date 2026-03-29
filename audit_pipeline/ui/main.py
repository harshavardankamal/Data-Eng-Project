from __future__ import annotations

import pandas as pd
import streamlit as st

from audit_pipeline.services.blob_store import StorageNotConfiguredError
from audit_pipeline.services.catalog_store import load_catalog, load_latest_snapshot_metadata
from audit_pipeline.services.snapshot_store import load_snapshot_dataframe
from audit_pipeline.settings import SNAPSHOT_CACHE_TTL_SECONDS
from audit_pipeline.ui.components import (
    apply_custom_css,
    render_architecture_strip,
    render_bsg_flow,
    render_metrics_panel,
    render_query_panel,
    render_snapshot_slider,
)


@st.cache_data(ttl=SNAPSHOT_CACHE_TTL_SECONDS)
def load_catalog_cached() -> list[dict]:
    return load_catalog()


@st.cache_data(ttl=SNAPSHOT_CACHE_TTL_SECONDS)
def load_latest_snapshot_cached() -> dict | None:
    return load_latest_snapshot_metadata()


@st.cache_data(ttl=SNAPSHOT_CACHE_TTL_SECONDS)
def load_snapshot_cached(snapshot_id: str, layer: str) -> pd.DataFrame:
    return load_snapshot_dataframe(snapshot_id, layer)


def load_pair(layer: str, snapshot_id: str, latest_snapshot_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    return load_snapshot_cached(snapshot_id, layer), load_snapshot_cached(latest_snapshot_id, layer)


def main() -> None:
    st.set_page_config(
        page_title="Open Table Format Time Travel Audit Pipeline",
        page_icon=":satellite:",
        layout="wide",
    )
    apply_custom_css()
    st.title("Open Table Format Time Travel Audit Pipeline")
    st.caption("Read-only explorer for daily published telemetry snapshots stored in Azure Blob Storage and queried with stateless DuckDB compute.")

    try:
        catalog = load_catalog_cached()
        latest_snapshot_meta = load_latest_snapshot_cached()
    except StorageNotConfiguredError as exc:
        st.error(str(exc))
        st.info("Configure Azure Blob Storage in your app settings, then redeploy the Streamlit app and the refresh job.")
        st.stop()

    with st.sidebar:
        st.header("Published Snapshot")
        st.caption("This app is read-only. A separate daily refresh job publishes new snapshots to Blob Storage.")
        if latest_snapshot_meta:
            st.metric("Latest Snapshot", latest_snapshot_meta["latest_snapshot_id"])
            st.caption(f"Published at: {latest_snapshot_meta['snapshot_created_at']}")
            st.caption(f"Stations: {', '.join(latest_snapshot_meta['station_ids'])}")
            st.caption(
                "Row counts: "
                f"Bronze {latest_snapshot_meta['row_counts']['bronze']:,}, "
                f"Silver {latest_snapshot_meta['row_counts']['silver']:,}, "
                f"Gold {latest_snapshot_meta['row_counts']['gold']:,}"
            )

    if not catalog:
        st.warning("No published snapshots were found in Blob Storage. Run the daily refresh job once to create the first snapshot.")
        st.stop()

    selected_snapshot_id = render_snapshot_slider(catalog)
    latest_snapshot = catalog[-1]
    selected_layer = st.selectbox("Displayed data layer", ["bronze", "silver", "gold"], index=2)

    render_architecture_strip()
    st.subheader("Bronze → Silver → Gold Movement")
    render_bsg_flow(latest_snapshot)

    previous_df, current_df = load_pair(selected_layer, selected_snapshot_id, latest_snapshot["snapshot_id"])
    compare_cols = st.columns(2)
    with compare_cols[0]:
        st.markdown(f"**Past Snapshot {selected_snapshot_id}**")
        st.dataframe(previous_df.head(25), use_container_width=True, height=320)
    with compare_cols[1]:
        st.markdown(f"**Current Snapshot {latest_snapshot['snapshot_id']}**")
        st.dataframe(current_df.head(25), use_container_width=True, height=320)

    render_query_panel(selected_layer, previous_df, current_df)
    render_metrics_panel(latest_snapshot)
    st.caption(f"Latest snapshot: {latest_snapshot['snapshot_id']} from {latest_snapshot['created_at']}")


if __name__ == "__main__":
    main()
