from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv

from src.storage.blob import latest_blob, upload_bytes, download_parquet


PROCESSED_PREFIX_DEFAULT = "processed/hicp/prc_hicp_midx/geo=LU/coicop=CP00/"
QUALITY_PREFIX_DEFAULT = "metadata/quality/hicp/prc_hicp_midx/geo=LU/coicop=CP00/"


def to_python(obj):
    """
    Recursively convert numpy / pandas types to pure Python types.
    """
    import numpy as np
    import pandas as pd

    if isinstance(obj, dict):
        return {k: to_python(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_python(v) for v in obj]
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    else:
        return obj


def _monthly_frequency_ok(times: pd.Series) -> bool:
    """Check if time series looks monthly without gaps (for LU/CP00/unit single series)."""
    t = pd.to_datetime(times.dropna()).sort_values().unique()
    if len(t) < 3:
        return True  # not enough info to judge

    # expected monthly sequence from min to max
    expected = pd.date_range(start=t[0], end=t[-1], freq="MS")  # month start
    return len(expected) == len(t) and (expected.values == t).all()


def run_checks(df: pd.DataFrame) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    passed = True

    # 1) Schema
    required_cols = ["time", "geo", "coicop", "unit", "value"]
    missing = [c for c in required_cols if c not in df.columns]
    checks.append({"name": "schema_required_columns", "passed": len(missing) == 0, "missing": missing})
    passed &= (len(missing) == 0)

    # 2) Non-null
    if len(missing) == 0:
        null_counts = df[required_cols].isna().sum().to_dict()
        checks.append({"name": "non_null_required_columns", "passed": all(v == 0 for v in null_counts.values()), "null_counts": null_counts})
        passed &= all(v == 0 for v in null_counts.values())

    # 3) Duplicate key check
    key_cols = [c for c in ["time", "geo", "coicop", "unit"] if c in df.columns]
    if key_cols:
        dup_count = int(df.duplicated(subset=key_cols).sum())
        checks.append({"name": "no_duplicate_keys", "passed": dup_count == 0, "duplicate_rows": dup_count, "key_cols": key_cols})
        passed &= (dup_count == 0)

    # 4) Value sanity (HICP index should be > 0)
    if "value" in df.columns:
        bad = df["value"].isna().sum()
        non_positive = int((df["value"] <= 0).sum())
        checks.append({"name": "value_positive", "passed": non_positive == 0, "non_positive": non_positive})
        passed &= (non_positive == 0)

    # 5) Time parsing ok
    if "time" in df.columns:
        time_na = int(pd.to_datetime(df["time"], errors="coerce").isna().sum())
        checks.append({"name": "time_parseable", "passed": time_na == 0, "unparseable_time_rows": time_na})
        passed &= (time_na == 0)

    # 6) Monthly frequency (for LU/CP00/unit this should be one series)
    if all(c in df.columns for c in ["time", "geo", "coicop", "unit"]):
        # If multiple units/series exist, check each group
        group_cols = ["geo", "coicop", "unit"]
        freq_pass = True
        freq_details = []
        for (g, c, u), sub in df.groupby(group_cols):
            ok = _monthly_frequency_ok(sub["time"])
            freq_pass &= ok
            if not ok:
                freq_details.append({"geo": g, "coicop": c, "unit": u, "issue": "missing_months_or_duplicates"})
        checks.append({"name": "monthly_frequency_no_gaps", "passed": freq_pass, "details": freq_details})
        passed &= freq_pass

    # Summary stats (helpful in report)
    summary = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "min_time": None,
        "max_time": None,
        "value_min": None,
        "value_max": None,
    }
    if "time" in df.columns:
        t = pd.to_datetime(df["time"], errors="coerce")
        summary["min_time"] = None if t.isna().all() else t.min().date().isoformat()
        summary["max_time"] = None if t.isna().all() else t.max().date().isoformat()
    if "value" in df.columns:
        summary["value_min"] = None if df["value"].isna().all() else float(df["value"].min())
        summary["value_max"] = None if df["value"].isna().all() else float(df["value"].max())

    return {"passed": bool(passed), "checks": checks, "summary": summary}


def main() -> None:
    load_dotenv()

    processed_prefix = os.getenv("HICP_PROCESSED_PREFIX", PROCESSED_PREFIX_DEFAULT)
    quality_prefix = os.getenv("HICP_QUALITY_PREFIX", QUALITY_PREFIX_DEFAULT)

    processed_blob = latest_blob(processed_prefix)
    if not processed_blob:
        raise RuntimeError(f"No processed parquet found under prefix: {processed_prefix}")

    df = download_parquet(processed_blob)
    report = run_checks(df)

    report_meta = {
        "processed_blob": processed_blob,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "pipeline_stage": "silver/quality",
    }
    out = {"meta": report_meta, "report": report}

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    status = "PASS" if report["passed"] else "FAIL"
    report_blob = f"{quality_prefix}ts={ts}_{status}.json"

    clean_out = to_python(out)

    upload_bytes(
        json.dumps(clean_out, ensure_ascii=False, indent=2).encode("utf-8"),
        report_blob,
        content_type="application/json",
    )

    # Also write a "latest" pointer file (super useful in pipelines)
    latest_blob_path = f"{quality_prefix}LATEST.json"
    
    
    upload_bytes(json.dumps({"latest_report": report_blob}, indent=2).encode("utf-8"), latest_blob_path, content_type="application/json")

    print("✅ Quality check complete")
    print(f"   Processed: {processed_blob}")
    print(f"   Report:    {report_blob}")
    print(f"   Status:    {status}")


if __name__ == "__main__":
    main()

