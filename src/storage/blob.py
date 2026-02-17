from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient



def get_blob_service_client() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env")
    return BlobServiceClient.from_connection_string(conn_str)


def get_container_name() -> str:
    return os.getenv("AZURE_BLOB_CONTAINER", "eurostat")


def upload_bytes(data: bytes, blob_path: str, content_type: str | None = None) -> None:
    bsc = get_blob_service_client()
    container = get_container_name()
    blob_client = bsc.get_blob_client(container=container, blob=blob_path)
    blob_client.upload_blob(data, overwrite=True, content_type=content_type)


def download_bytes(blob_path: str) -> bytes:
    bsc = get_blob_service_client()
    container = get_container_name()
    blob_client = bsc.get_blob_client(container=container, blob=blob_path)
    return blob_client.download_blob().readall()


@dataclass(frozen=True)
class BlobItemInfo:
    name: str
    last_modified: str  # ISO-like string


def list_blobs(prefix: str) -> List[BlobItemInfo]:
    bsc = get_blob_service_client()
    container = get_container_name()
    container_client = bsc.get_container_client(container)

    items: List[BlobItemInfo] = []
    for b in container_client.list_blobs(name_starts_with=prefix):
        # b.last_modified is datetime; convert to sortable string
        items.append(BlobItemInfo(name=b.name, last_modified=b.last_modified.isoformat()))
    return items


def latest_blob(prefix: str) -> Optional[str]:
    blobs = list_blobs(prefix)
    if not blobs:
        return None
    blobs.sort(key=lambda x: x.last_modified)
    return blobs[-1].name

 
def download_parquet(blob_path: str) -> pd.DataFrame:
    data = download_bytes(blob_path)
    return pd.read_parquet(io.BytesIO(data))