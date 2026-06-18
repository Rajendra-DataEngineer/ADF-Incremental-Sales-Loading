# Incremental Sales Data Loading — Azure Data Factory + Databricks

Production-oriented medallion architecture with watermark-based incremental loading, centralized error logging, schema drift handling, and PySpark Delta Lake MERGE.

## Architecture

```
Bronze (CSV)  ──►  Silver (Data Flow + drift)  ──►  Gold (aggregate + join)
       │                    │                              │
       └──── Incremental (watermark + upsert) ──────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
            ADF Mapping Data Flow          Databricks Delta MERGE
                    │                               │
                    └───────────► Azure SQL / Delta Tables
```

## Key Features

### 1. Error Handling & Logging Framework
- Reusable `pl_error_handler` pipeline decoupled from business logic
- On activity failure, parent pipelines call `Execute Pipeline` → `pl_error_handler`
- Logs `PipelineName`, `ActivityName`, `ErrorMessage`, `RunId`, `ErrorTimestamp`
- Immutable CSV per failure written to `monitoring/errors/error_{RunId}.csv`

### 2. Incremental Sales Pipeline (fixed)
- **Lookup runs first** — fetches `MAX(LastModifiedDate)` watermark
- **Data Flow filters** new records: `saledate > $WatermarkValue`
- **Upsert** to `Sales_Target` on key `SaleID` (no duplicate inserts)
- **Watermark update** via `[dbo].[UpdateWatermarkValue]` stored procedure
- **Retries**: 2 on all critical activities
- **Activity name casing** fixed (`Lookup_lastdate` consistent everywhere)

### 3. Schema Drift Handling (Mapping Data Flows)
Both `df_customer_bronze_to_silver` and `df_silver_to_gold` include:
- `allowSchemaDrift: true` + `inferDriftedColumnTypes: true`
- `coalesce(byName(...))` column normalization for drifted names
- `assert` on required columns
- `split` valid vs invalid rows
- Quarantine sink for bad rows (`quarantine/bronze/customers`, `quarantine/silver/sales`)

### 4. PySpark Delta Lake MERGE
- Notebook: `notebooks/incremental_sales_delta_merge.py`
- ADF pipeline: `pl_databricks_delta_merge` (DatabricksNotebook activity)
- MERGE on `SaleID`, update only when source `LastModifiedDate` is newer
- Watermark stored in Delta table `main.sales.watermark_table`

## Project Structure

```
adf-incremental-sales-loading/
├── dataflow/
│   ├── df_customer_bronze_to_silver.json   # Bronze→Silver with drift + quarantine
│   ├── df_incremental_sales_load.json     # Watermark filter + SQL upsert
│   └── df_silver_to_gold.json             # Silver→Gold with join + drift
├── dataset/
│   ├── ds_error_log.json                  # Error log sink (parameterized filename)
│   ├── ds_quarantine_bronze_customers.json
│   └── ds_quarantine_silver_sales.json
├── pipeline/
│   ├── 01_Incremental_sales_pipeline.json # Watermark incremental (fixed)
│   ├── pl_error_handler.json              # Centralized error logging
│   ├── pl_customer_bronze_to_silver.json
│   ├── pl_silver_to_gold.json
│   └── pl_databricks_delta_merge.json     # Databricks Delta MERGE
├── notebooks/
│   └── incremental_sales_delta_merge.py     # PySpark Delta MERGE
└── sql/
    └── setup_incremental_sales.sql          # Tables + stored procedures
```

## Prerequisites

1. Run `sql/setup_incremental_sales.sql` in Azure SQL Database
2. Create ADLS containers: `bronze`, `silver`, `gold`, `monitoring`, `quarantine`
3. Update Linked Services with your credentials
4. Upload notebook to Databricks workspace (path in `pl_databricks_delta_merge`)

## How to Run

| Pipeline | Purpose |
|----------|---------|
| `01_Incremental_sales_pipeline` | ADF watermark incremental load to Azure SQL |
| `pl_customer_bronze_to_silver` | Bronze→Silver with schema drift |
| `pl_silver_to_gold` | Silver→Gold aggregation |
| `pl_databricks_delta_merge` | PySpark Delta MERGE on Databricks |
| `pl_error_handler` | Called automatically on failures (do not run standalone) |

## Cost Optimization

| Setting | Value | Rationale |
|---------|-------|-----------|
| Data Flow `coreCount` | 4 | Right-sized for dev/small datasets |
| `traceLevel` | Coarse | Lower log volume in production |
| `retry` | 2 | Resilience without excessive re-runs |
| Error logs | One file per RunId | Append-safe, no overwrite conflicts |

## Error Log Format

Each failure produces `monitoring/errors/error_{RunId}.csv`:

| Column | Description |
|--------|-------------|
| PipelineName | Parent pipeline that failed |
| ActivityName | Failed activity name |
| ErrorMessage | ADF error message |
| RunId | Pipeline run GUID |
| ErrorTimestamp | UTC timestamp |

## Learnings

- Watermark pattern with dependency ordering (Lookup → Load → Update)
- Centralized try/catch via `Execute Pipeline` + `Failed` dependency
- Schema drift: `byName` + `assert` + `split` + quarantine
- Delta MERGE as modern alternative to SQL upsert
- Cost-aware Data Flow compute settings

---
**Made by Rajendra K** — Aspiring Azure Data Engineer | Open to UK Relocation
