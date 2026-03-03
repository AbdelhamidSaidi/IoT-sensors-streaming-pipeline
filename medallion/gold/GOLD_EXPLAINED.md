# Gold Layer (`transform_gold`) + Dim Pipeline

This document explains:
- `transform_gold` (implemented in `medallion/gold/gold.py`) — a streaming transformer that writes the Gold layer.
- `dim` (implemented in `medallion/gold/dim.py`) — a downstream process that consumes `gold_central` and appends rows into `dim_<metric>` tables.

It's written for engineers/operators who deploy, maintain, or extend the gold + dim layers.

## Purpose
- Produce analytics-ready, domain-specific gold tables from the silver data.
- Maintain a central `gold_central` table with a `status` column and derived
  tables for device families: `gold_vent`, `gold_humidifier`, `gold_thermometer`.
- Stream transformations live so downstream consumers can query up-to-date
  state without reprocessing the full silver table.

In addition, the dim pipeline:
- Creates a canonical `dim_sensor` table and enforces foreign keys.
- Appends event rows into `dim_<metric>` tables from `gold_central`.

## Tables Created
- `dim_sensor`: sensor dimension table (`sensor_id` is the primary key). This is the canonical list of sensors.
- `gold_central`: canonical table containing `sensor_id`, `metric`, `value`,
  `unit`, `ts`, `status`, and `ingestion_time`. Acts as the authoritative gold
  feed.
- `gold_vent`, `gold_humidifier`, `gold_thermometer`: derived tables that store
  rows filtered by metric and reference `gold_central(id)` via `central_id`.

### Relationships (PK/FK)
- `dim_sensor(sensor_id)` is the primary key.
- `gold_central.sensor_id` has a foreign key to `dim_sensor(sensor_id)`.
- Each derived table `gold_<metric>.central_id` has a foreign key to `gold_central(id)`.
- Each appended dim table `dim_<metric>.sensor_id` has a foreign key to `dim_sensor(sensor_id)`.

## Metric mapping
- The gold layer classifies incoming `sensor_id` values into metrics using a
  naming convention:
  - `vent` → `vent`
  - strings containing `humid` → `humidifier`
  - strings containing `therm` or `thermostat` → `thermometer`
  - unknown patterns → `unknown` (stored in `gold_central`, not copied to derived tables)

## Status Logic
- Each gold row includes a `status` computed against a per-metric spec:
  - `ok`: value within [min, max]
  - `warning`: value > max
  - `idle`: value < min
  - `unknown`: missing metric or value
- Default specs are defined in `DEFAULT_SPECS` in the code and can be moved to
  a configuration table or JSON file for production.

## Streaming Behavior
- The transformer runs continuously and polls `sensor_logs_silver` for new rows.
- By default it polls every 5 seconds; the interval is configurable via
  `--interval` (seconds) when launching `medallion/gold/gold.py`.
- It maintains `last_ts` (derived from `gold_central`) and only processes rows
  with `ts > last_ts` to ensure incremental, idempotent ingestion.

### `transform_gold` implementation note
In code, `transform_gold` is the Gold streaming loop inside `build_gold_layer()` in `medallion/gold/gold.py`.
It also upserts `dim_sensor` (insert if missing, update `last_seen`) before inserting to `gold_central`.

## How it links to Silver
- The gold layer reads canonical, cleaned rows from `sensor_logs_silver` and
  derives metrics by mapping `sensor_id` and evaluating the `status` logic.
- It commits gold rows in the same transaction and updates `last_ts` after a
  successful batch to avoid skipping or duplicating data.

## Running the gold stream
1. Ensure `DB_CONFIG` in `medallion/silver/silver.py` is configured and that the
   underlying SQL Server is reachable.
2. Run the gold transformer (example):

```powershell
python medallion\gold\gold.py --interval 5
```

3. Verify tables exist and inspect `gold_central` and derived tables using your
   SQL tool of choice.

## Dim pipeline (`dim.py`)

### What it does
- Reads from the central gold table: `gold_central`.
- Ensures the required tables exist:
  - `dim_sensor`
  - `dim_gold_watermark` (global watermark used by the dim process)
  - `dim_<metric>` for each metric present in `gold_central`
- Appends rows into `dim_<metric>` (event-style): `(sensor_id, ts, value, unit)`.
- Advances `dim_gold_watermark.last_ts` to the maximum processed `ts`.

### Important: append-only behavior
The current dim implementation is append-only: new gold rows become new rows in `dim_<metric>`.
So row counts will grow over time.

### Running dim
Run continuously:

```powershell
python medallion\gold\dim.py --interval 5
```

Run once (useful for sanity checks):

```powershell
python medallion\gold\dim.py --once
```

## Operational notes & improvements
- Move `DEFAULT_SPECS` to a managed config (DB table or JSON) so specs can be
  updated without code changes.
- Add monitoring/metrics (counts of `ok`, `warning`, `idle`, `unknown`) to a
  metrics sink (Prometheus, logs, or a dashboard).
- Consider adding a unique constraint on `(sensor_id, ts)` in `dim_<metric>` if you want strict de-duplication.
- Consider per-sensor watermarks if sensors have highly variable ingestion rates
  and you want independent progress tracking.
- Add retry/backoff and alerting for persistent DB connectivity failures.

## Security
- Avoid storing credentials in source files — use environment variables or a
  secrets manager. When using Integrated Authentication, ensure the process
  principal has minimal required privileges.

If you'd like, I can add a `GOLD_EXPLAINED.md` section with example queries and
sample dashboard suggestions next.
