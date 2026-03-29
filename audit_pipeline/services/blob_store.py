from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from audit_pipeline.settings import AZURE_BLOB_CONTAINER, AZURE_STORAGE_CONNECTION_STRING


class StorageNotConfiguredError(RuntimeError):
    pass


def _require_connection_string() -> str:
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise StorageNotConfiguredError(
            "Set AZURE_STORAGE_CONNECTION_STRING before running the app or refresh job."
        )
    return AZURE_STORAGE_CONNECTION_STRING


@lru_cache(maxsize=1)
def _service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(_require_connection_string())


def get_container_client(create: bool = False):
    container = _service_client().get_container_client(AZURE_BLOB_CONTAINER)
    if create:
        try:
            container.create_container()
        except ResourceExistsError:
            pass
    return container


def upload_bytes(blob_name: str, data: bytes, content_type: str | None = None) -> None:
    blob_client = get_container_client(create=True).get_blob_client(blob_name)
    content_settings = ContentSettings(content_type=content_type) if content_type else None
    blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)


def download_bytes(blob_name: str) -> bytes:
    blob_client = get_container_client(create=False).get_blob_client(blob_name)
    try:
        return blob_client.download_blob().readall()
    except ResourceNotFoundError as exc:
        raise FileNotFoundError(blob_name) from exc


def blob_exists(blob_name: str) -> bool:
    blob_client = get_container_client(create=False).get_blob_client(blob_name)
    return blob_client.exists()


def upload_json(blob_name: str, payload: Any) -> None:
    upload_bytes(blob_name, json.dumps(payload, indent=2).encode("utf-8"), content_type="application/json")


def download_json(blob_name: str, default: Any = None) -> Any:
    try:
        return json.loads(download_bytes(blob_name).decode("utf-8"))
    except FileNotFoundError:
        return default
