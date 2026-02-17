# 📦 European Public Data Pipeline

A hybrid Azure-based data engineering pipeline for ingesting, validating, transforming, and warehousing European public macroeconomic data from Eurostat (HICP – Harmonised Index of Consumer Prices).

This project demonstrates end-to-end cloud data architecture using Azure Blob Storage and Azure SQL Database.

## 🚀 Project Overview

This pipeline ingests official Eurostat macroeconomic data and processes it through a structured multi-layer architecture:

Eurostat API (JSON-stat)
→ Azure Blob Storage (Bronze – Raw JSON)
→ Azure Blob Storage (Silver – Parquet)
→ Data Quality Validation
→ Azure SQL Database (Gold – Warehouse Layer)

The pipeline is fully modular, environment-driven, and cloud-ready.

## 🏗 Architecture

                 ┌─────────────────────┐
                 │  Eurostat API       │
                 │  (JSON-stat)        │
                 └──────────┬──────────┘
                            │
                            ▼
               ┌─────────────────────────┐
               │ Azure Blob Storage      │
               │ Bronze (Raw JSON)       │
               └──────────┬──────────────┘
                          │
                          ▼
               ┌─────────────────────────┐
               │ Azure Blob Storage      │
               │ Silver (Parquet)        │
               └──────────┬──────────────┘
                          │
                          ▼
               ┌─────────────────────────┐
               │ Data Quality Layer      │
               │ Validation + Reports    │
               └──────────┬──────────────┘
                          │
                          ▼
               ┌─────────────────────────┐
               │ Azure SQL Database      │
               │ Gold (fact_hicp)        │
               └─────────────────────────┘
## 🧰 Tech Stack

Python 3.11+

Pandas

Azure Blob Storage

Azure SQL Database

SQLAlchemy + pyodbc

JSON-stat parsing

Parquet (pyarrow)

Git + GitHub

## 📂 Project Structure

```text
european-public-data-pipeline/
│
├── src/
│   ├── ingestion/
│   │   ├── ingestion_hicp_raw.py
│   │   └── process_hicp_silver.py
│   │
│   ├── quality/
│   │   └── check_hicp_quality.py
│   │
│   ├── db/
│   │   ├── sql.py
│   │   └── load_hicp_to_sql.py
│   │
│   ├── storage/
│   │   └── blob.py
│   │
│   └── __init__.py
│
├── .env.example
├── requirements.txt
├── README.md
└── .gitignore
```

## 🔄 Pipeline Stages
### 1️⃣ Bronze — Raw Ingestion

Fetch HICP data from Eurostat and store full JSON-stat payload in Azure Blob Storage.

python -m src.ingestion.ingestion_hicp_raw

Output example:

raw/hicp/prc_hicp_midx/geo=LU/coicop=CP00/ts=20260213_130854.json
### 2️⃣ Silver — Structured Processing

Parse JSON-stat into a structured Parquet dataset.

python -m src.ingestion.process_hicp_silver

Output schema:

Column	Description
time	Monthly observation date
geo	Country code
coicop	COICOP classification
unit	Index unit (e.g. I15)
value	Inflation index value
processed_at_utc	Processing timestamp
raw_blob	Reference to raw data source
### 3️⃣ Data Quality Layer

Validation checks include:

Null value detection

Duplicate primary keys

Numeric value validation

Time continuity

Structural consistency

python -m src.quality.check_hicp_quality

If validation fails, SQL loading is blocked.

### 4️⃣ Gold — SQL Warehouse Load

Load validated data into Azure SQL:

python -m src.db.load_hicp_to_sql

Target table:

dbo.fact_hicp

This table is optimized for analytical querying and dashboard integration.

## ⚙ Configuration

Create a .env file in the root directory:

AZURE_STORAGE_CONNECTION_STRING=
AZURE_BLOB_CONTAINER=eurostat

AZURE_SQL_SERVER=your-server.database.windows.net
AZURE_SQL_DATABASE=europubdata_db
AZURE_SQL_USERNAME=your_admin
AZURE_SQL_PASSWORD=your_password

## 📊 Current Dataset

Dataset: prc_hicp_midx
Geography: Luxembourg (LU)
COICOP: CP00 (All items)
Frequency: Monthly
Unit: Index (2015 = 100)
