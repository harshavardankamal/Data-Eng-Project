# Hosting And Update Strategy

This repo is now designed around one cheap Azure-friendly hosting mode.

## Recommended Deployment

- Streamlit app as a read-only explorer
- one scheduled daily refresh job
- Azure Blob Storage as the persisted source of truth
- DuckDB only for in-memory compute during app requests and CLI queries

## Why This Shape Fits A Student Account

- the app is not constantly polling the source
- refresh runs only once per 24 hours unless forced
- Blob Storage is cheaper than keeping a database running
- the UI and the writer are separated, so the app stays lightweight

## Responsibilities

### Streamlit app

- reads `metadata/catalog.json`
- reads `metadata/latest_snapshot.json`
- loads snapshot parquet files from Blob
- runs comparisons and SQL with DuckDB in memory
- never fetches source data directly

### Refresh job

- checks if the latest snapshot is older than 24 hours
- fetches official NOAA/NWS records when due
- writes raw, Bronze, Silver, and Gold parquet snapshots to Blob
- updates latest snapshot metadata

## Suggested Azure Services

- Azure Container Apps or Azure App Service for Streamlit
- Azure Functions Timer or another scheduled job for `python -m audit_pipeline.jobs.refresh_live_data`
- Azure Blob Storage for all data and metadata

## Suggested First Deployment Order

1. Create an Azure Storage account and blob container
2. Set `AZURE_STORAGE_CONNECTION_STRING` and `AZURE_BLOB_CONTAINER`
3. Run the refresh job once to publish the first snapshot
4. Deploy Streamlit with the same blob settings
5. Add the daily timer job

## Future Upgrade Path

If you later want a stronger “open table format” story, add Iceberg as a storage/backend upgrade without changing the Streamlit UI.
