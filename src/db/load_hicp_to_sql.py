from __future__ import annotations

import json
import os
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.sql import get_sql_engine
from src.storage.blob import download_bytes, download_parquet


QUALITY_LATEST_PTR = "metadata/quality/hicp/prc_hicp_midx/geo=LU/coicop=CP00/LATEST.json"


def get_latest_pass_processed_blob() -> str:
    """
    Read LATEST.json pointer and ensure last quality run was PASS.
    """
    ptr = json.loads(download_bytes(QUALITY_LATEST_PTR).decode("utf-8"))
    report_blob = ptr["latest_report"]

    if "_PASS.json" not in report_blob:
        raise RuntimeError(f"Latest quality report is not PASS: {report_blob}")

    report = json.loads(download_bytes(report_blob).decode("utf-8"))
    return report["meta"]["processed_blob"]


def ensure_table(engine) -> None:
    ddl = """
    IF OBJECT_ID('dbo.fact_hicp', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.fact_hicp (
            [time] DATE NOT NULL,
            geo NVARCHAR(10) NOT NULL,
            coicop NVARCHAR(20) NOT NULL,
            unit NVARCHAR(20) NOT NULL,
            value FLOAT NULL,
            processed_at_utc NVARCHAR(40) NOT NULL,
            raw_blob NVARCHAR(300) NOT NULL
        );

        CREATE UNIQUE INDEX UX_fact_hicp_key
            ON dbo.fact_hicp ([time], geo, coicop, unit);
    END
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def delete_existing_series(engine, geo: str, coicop: str, unit: str) -> None:
    sql = """
    DELETE FROM dbo.fact_hicp
    WHERE geo = :geo
      AND coicop = :coicop
      AND unit = :unit
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"geo": geo, "coicop": coicop, "unit": unit})


def main() -> None:
    load_dotenv()

    # 1) Identify latest PASS parquet
    processed_blob = get_latest_pass_processed_blob()

    # 2) Load parquet from Blob
    df = download_parquet(processed_blob)

    # Keep only table columns
    cols = ["time", "geo", "coicop", "unit", "value", "processed_at_utc", "raw_blob"]
    df = df[cols].copy()

    # Convert to SQL compatible types
    df["time"] = pd.to_datetime(df["time"]).dt.date

    geo = str(df["geo"].iloc[0])
    coicop = str(df["coicop"].iloc[0])
    unit = str(df["unit"].iloc[0])

    engine = get_sql_engine()

    # 3) Create table if missing
    ensure_table(engine)

    # 4) Idempotent load (delete + insert)
    delete_existing_series(engine, geo, coicop, unit)

    df.to_sql(
        "fact_hicp",
        con=engine,
        schema="dbo",
        if_exists="append",
        index=False,
        chunksize=200,
        method="multi"
    )

    print("✅ HICP successfully loaded into Azure SQL")
    print(f"   Processed blob: {processed_blob}")
    print(f"   Rows inserted:  {len(df)}")
    print(f"   Series:         geo={geo}, coicop={coicop}, unit={unit}")


if __name__ == "__main__":
    main()