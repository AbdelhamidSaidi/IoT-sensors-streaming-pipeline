# Silver Layer: Transformation & Storage

This document explains the cleaning, normalization, imputation, and streaming
behavior implemented by `medallion/silver/silver.py`. It's aimed at engineers and
operators who maintain or extend the silver-layer that converts the bronze
`sensor_logs` table into the analytics-ready `sensor_logs_silver` table in the
same SQL Server database.

## Purpose
- Move raw, possibly noisy sensor records from the bronze table (`sensor_logs`) into
  a canonical silver table (`sensor_logs_silver`).
- Apply defensive parsing, unit normalization, timestamp validation, and
  imputation rules so downstream consumers get consistent, analytics-ready data.
- Stream the pipeline incrementally using a persisted watermark (`silver_watermark`).

## Inputs
- Source table: `sensor_logs` (bronze)
  - Expected columns read by the script: `id`, `sensor_id`, `val`, `unit`, `ts`.
  - Records are produced by the Kafka consumer and appended to this table.

## Outputs
- Target table: `sensor_logs_silver`
  - Columns: `id (IDENTITY)`, `sensor_id (NVARCHAR)`, `value (FLOAT)`,
    `unit (NVARCHAR)`, `ts (DATETIME2)`, `data_quality (NVARCHAR)`,
    `ingestion_time (DATETIME2 DEFAULT SYSUTCDATETIME())`.
  - `data_quality` values used by the pipeline: `clean`, `dirty`, `imputed`.

## Key Transformation Rules

- Value parsing
  - `val` is accepted as numeric or stringified numeric; the script attempts `float(val)`.
  - If `val` parses, it's stored in `value` and considered `ok`.
  - If `val` is missing or unparseable, the pipeline will attempt to impute a
    numeric value using per-sensor averages (see Imputation below). If imputation
    is not possible, `value` is NULL and the record is `dirty`.

- Unit normalization
  - Common unit strings are normalized to a controlled vocabulary:
    - Temperature variants → `Celsius`.
    - Percentage/humidity variants → `Percentage`.
  - Unknown or empty unit strings are left NULL or preserved when meaningful.

- Timestamp parsing
  - The pipeline accepts `YYYY-MM-DD HH:MM:SS` and ISO formats via `datetime.fromisoformat()`.
  - Only records with a valid `ts` greater than the current watermark are processed;
    invalid or missing `ts` result in `ts = NULL` and `data_quality = dirty`.

- Data quality
  - `clean`: `sensor_id` present and both `value` and `ts` parsed successfully.
  - `imputed`: `value` was missing/bad but a numeric value was filled using the
    per-sensor average (and `ts` parsed successfully).
  - `dirty`: missing `sensor_id`, unparseable `ts`, or neither parsing nor imputation succeeded.

## Imputation (Fill NULL values with averages)
- The pipeline computes per-sensor averages from already-stored silver rows
  (preferred) and falls back to averages computed from the current bronze batch
  when necessary.
- When a numeric `val` is missing or invalid and a per-sensor average is available,
  the pipeline fills `value` with that average and sets `data_quality = 'imputed'`.
- Imputation is intentionally conservative: only numeric values are imputed and
  the record's timestamp must be valid for the row to be considered `imputed`.

## Streaming & Watermarking Behavior
- The script is stream-friendly: it runs continuously and polls the bronze table
  every 5 seconds (configurable in the script).
- A watermark is stored in the `silver_watermark` table (`last_ts`) and the
  pipeline only processes bronze rows with `ts > last_ts`.
- After each successful batch the watermark is updated to the maximum `ts`
  processed and the transaction is committed. This keeps processing incremental
  and idempotent for most append-only bronze workloads.

## Connection Diagnostics
- The script discovers installed ODBC drivers via `pyodbc.drivers()` and tries a
  set of sensible connection-string variants (host:port, tcp:host, common named
  instances) to maximize compatibility with different SQL Server setups.
- If no suitable ODBC driver is available the script raises a clear error asking
  you to install the Microsoft ODBC Driver for SQL Server (17 or 18).

## Error Handling & Reliability
- Each processing iteration runs inside a transaction. On exception the transaction
  is rolled back and the iteration sleeps before retrying.
- The watermark is only updated after a successful commit, preventing data loss
  or skipping records on intermittent failures.

## Performance & Scaling Considerations
- Process rows in small batches when bronze volumes are large; the current
  implementation reads all bronze rows and filters by watermark — consider a
  paged query (WHERE ts > watermark ORDER BY ts OFFSET/FETCH) for production.
- For high-throughput scenarios consider partitioning/parallelizing per-sensor
  or sharding by time windows, and add a metrics sink for clean/imputed/dirty counts.

## Security & Operational Notes
- The script requires an ODBC driver and connectivity to SQL Server. For SQL
  authentication, use a secure secrets store instead of embedding credentials.
- When using Integrated Authentication (Trusted Connection), ensure the process
  identity has read/write permissions on the involved tables.

## How to run
1. Update `DB_CONFIG` in `medallion/silver/silver.py` with server, database, driver and auth.
2. Ensure `pyodbc` and a Microsoft ODBC Driver for SQL Server are installed.
3. Run the streaming pipeline from the repository root:

```powershell
python medallion\\silver\\silver.py
```

Tip: add a non-streaming (single-iteration) flag if you want to run one batch and exit.

## Example: Imputation
- Bronze row:

  | id | sensor_id     | val     | unit     | ts                  |
  |----|---------------|---------|----------|---------------------|
  | 42 | thermostat_01 | NULL    | Celsius  | 2026-03-03 12:05:00 |

- Silver row after processing (if an average exists):

  | sensor_id     | value  | unit    | ts                  | data_quality |
  |---------------|--------|---------|---------------------|--------------|
  | thermostat_01 | 21.3   | Celsius | 2026-03-03 12:05:00 | imputed      |

## Next improvements (suggested)
- Add a CLI flag to run a single iteration (non-streaming) for maintenance runs.
- Persist per-sensor watermarks if you need independent processing rates.
- Add unit tests for parsing/normalization and an integration test for a small
  in-memory sqlite bronze table to validate end-to-end behavior.

If you want, I can add the single-iteration flag and a small test harness next.
