from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

from src.storage.blob import upload_bytes




EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"


def fetch_eurostat_json(dataset: str, params: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    url = f"{EUROSTAT_BASE}/{dataset}"
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(
            f"Eurostat request failed: {r.status_code}\n"
            f"URL: {r.url}\n"
            f"Response (first 2000 chars): {r.text[:2000]}"
        )
    return r.json()


def try_fetch_with_fallbacks(dataset: str, base_params: Dict[str, Any], unit: Optional[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Try with unit first (if provided), then retry without unit if Eurostat rejects it.
    Returns (payload, final_params_used).
    """
    params = dict(base_params)
    if unit:
        params["unit"] = unit
        try:
            return fetch_eurostat_json(dataset, params), params
        except RuntimeError as e:
            # retry without unit
            print("⚠️ Request failed with unit parameter. Retrying without 'unit'...")
            print(str(e).splitlines()[0])  # short hint

    params = dict(base_params)
    return fetch_eurostat_json(dataset, params), params


def main() -> None:
    load_dotenv()

    dataset = os.getenv("EUROSTAT_HICP_DATASET", "prc_hicp_midx")
    geo = os.getenv("HICP_GEO", "LU")
    coicop = os.getenv("HICP_COICOP", "CP00")
    unit: Optional[str] = os.getenv("HICP_UNIT", "I15")

    # Keep it small: start with Luxembourg + all-items HICP.
    base_params: Dict[str, Any] = {"geo": geo, "coicop": coicop}

    payload, final_params = try_fetch_with_fallbacks(dataset, base_params, unit)

    meta = {
        "dataset": dataset,
        "params": final_params,
        "requested_params": {**base_params, **({"unit": unit} if unit else {})},
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Eurostat dissemination API",
        "pipeline_stage": "bronze/raw",
    }
    wrapper = {"meta": meta, "data": payload}

    raw_bytes = json.dumps(wrapper, ensure_ascii=False).encode("utf-8")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_path = f"raw/hicp/{dataset}/geo={geo}/coicop={coicop}/ts={ts}.json"

    upload_bytes(raw_bytes, blob_path, content_type="application/json")
    print(f"✅ Uploaded raw HICP payload to Azure Blob:\n{blob_path}")


if __name__ == "__main__":
    main()