from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timezone
from itertools import product
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv

from src.storage.blob import download_bytes, latest_blob, upload_bytes


RAW_PREFIX_DEFAULT = "raw/hicp/prc_hicp_midx/geo=LU/coicop=CP00/"
PROCESSED_PREFIX_DEFAULT = "processed/hicp/prc_hicp_midx/geo=LU/coicop=CP00/"


def _ordered_category_codes(dim_obj: Dict[str, Any]) -> List[str]:
    """
    JSON-stat: dimension.<dim>.category.index is typically a dict {code: position}.
    We convert that to an ordered list of codes.
    """
    cat = dim_obj.get("category", {})
    idx = cat.get("index")

    if idx is None:
        return []

    # index can be dict (most common) or list (less common)
    if isinstance(idx, dict):
        return [k for k, _ in sorted(idx.items(), key=lambda kv: kv[1])]
    if isinstance(idx, list):
        return list(idx)

    raise TypeError(f"Unsupported category.index type: {type(idx)}")


def _parse_time_code(t: Any) -> Optional[pd.Timestamp]:
    """
    Eurostat JSON-stat often uses 'YYYYMmm' (e.g., '2024M01') or 'YYYY-MM'.
    Convert to a proper Timestamp (month start).
    """
    if t is None:
        return None
    s = str(t)

    m = re.match(r"^(\d{4})M(\d{2})$", s)
    if m:
        return pd.Timestamp(int(m.group(1)), int(m.group(2)), 1)

    # try common ISO-ish formats
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None

    # If only year-month, normalize to first day of month
    if ts.day == 1:
        return ts
    return pd.Timestamp(ts.year, ts.month, 1)


def jsonstat_to_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Eurostat Statistics API JSON-stat 2.0 payload to tidy DataFrame.
    Output columns: time, geo, coicop, unit (if present), value
    """
    if "dimension" not in payload or "value" not in payload:
        # helpful debug if API returned an error payload
        raise RuntimeError(f"Unexpected payload keys: {list(payload.keys())[:30]}")

    dim = payload["dimension"]
    dim_order: List[str] = payload["id"]  # dimension order
    sizes: List[int] = payload["size"]    # dimension sizes in same order
    values = payload["value"]             # flat array length = product(sizes)

    # Build ordered codes for each dimension
    codes_by_dim: List[List[str]] = []
    for d in dim_order:
        codes_by_dim.append(_ordered_category_codes(dim[d]))

    # Sanity check
    expected_len = 1
    for s in sizes:
        expected_len *= s
    if len(values) != expected_len:
        raise RuntimeError(f"Value length mismatch: got {len(values)}, expected {expected_len} from size={sizes}")

    # Cartesian product in dim_order; align with flat values order
    rows = []
    for i, combo in enumerate(product(*codes_by_dim)):
        row = {dim_order[j]: combo[j] for j in range(len(dim_order))}

        # JSON-stat "value" can be either:
        # - a list (dense)
        # - a dict with string keys "0", "1", ... (sparse)
        if isinstance(values, list):
            row["value"] = values[i]
        elif isinstance(values, dict):
            row["value"] = values.get(str(i))  # may be missing -> None
        else:
            raise TypeError(f"Unsupported 'value' type: {type(values)}")

        rows.append(row)

    df = pd.DataFrame(rows)

    # Normalize column set we care about
    if "time" in df.columns:
        df["time"] = df["time"].map(_parse_time_code)

    # Keep standard column order if present
    ordered_cols = [c for c in ["time", "geo", "coicop", "unit", "value"] if c in df.columns]
    df = df[ordered_cols].sort_values([c for c in ["geo", "coicop", "time"] if c in df.columns]).reset_index(drop=True)

    return df


def main() -> None:
    load_dotenv()

    raw_prefix = os.getenv("HICP_RAW_PREFIX", RAW_PREFIX_DEFAULT)
    processed_prefix = os.getenv("HICP_PROCESSED_PREFIX", PROCESSED_PREFIX_DEFAULT)

    raw_blob_name = latest_blob(raw_prefix)
    if not raw_blob_name:
        raise RuntimeError(f"No raw blobs found under prefix: {raw_prefix}")

    raw_bytes = download_bytes(raw_blob_name)
    wrapper = json.loads(raw_bytes.decode("utf-8"))
    payload = wrapper["data"]

    df = jsonstat_to_dataframe(payload)

    # Add pipeline metadata columns
    df["processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    df["raw_blob"] = raw_blob_name

    # Write parquet to memory
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    parquet_bytes = buf.getvalue()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    processed_blob_name = f"{processed_prefix}ts={ts}.parquet"
    upload_bytes(parquet_bytes, processed_blob_name, content_type="application/octet-stream")

    print("✅ Processed latest raw HICP (JSON-stat) to parquet and uploaded:")
    print(f"   Raw:       {raw_blob_name}")
    print(f"   Processed: {processed_blob_name}")
    print(f"   Rows:      {len(df):,}")
    print(f"   Columns:   {list(df.columns)}")


if __name__ == "__main__":
    main()