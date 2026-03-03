# Kafka Mini Project

This project demonstrates a minimal local Kafka setup (broker-only using KRaft) with a Python producer and consumer. The consumer persists incoming sensor messages into a SQL Server database (ODBC).

## Repository layout

- `app/data_generation/producer.py`: publishes synthetic sensor messages to the `sensor-data` topic.
- `app/consumer/consumer.py`: consumes messages from `sensor-data` and stores them in a SQL Server table named `sensor_logs`.
- `docker-compose.yaml`: runs an Apache Kafka broker (and optional services like Postgres/pgAdmin if present).

## What happens when you run things

1. Start Kafka
   - The `docker-compose.yaml` service `kafka` runs a single Kafka broker listening on `localhost:9092`.

2. Create the `sensor-data` topic (optional)
   - Kafka will auto-create topics by default, but you can explicitly create the topic inside the container:

```powershell
# start the broker
docker compose up -d
# create topic (inside the running Kafka container)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic sensor-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

3. Start the producer

```powershell
# inside a Python venv with dependencies installed
.venv\Scripts\python app\data_generation\producer.py
```

The producer sends a small batch of three synthetic sensor readings every second to the `sensor-data` topic.

4. Start the consumer

```powershell
.venv\Scripts\python app\consumer\consumer.py
```

- On startup the consumer connects to SQL Server using an ODBC connection string. It will read the connection string from the `MSSQL_CONN` environment variable if present; otherwise it prompts you to paste a connection string interactively.
- For each consumed message the consumer inserts a row into `sensor_logs` with columns: `sensor_id, val, unit, ts`.
- The consumer prints a `Stored: <sensor_id> at <timestamp>` line for each message persisted.

## Important implementation details and troubleshooting notes

- Python package: Use `kafka-python` and `pyodbc` for SQL Server connectivity. Install into your venv:

```powershell
.venv\Scripts\python -m pip install kafka-python pyodbc
```

- SQL Server ODBC driver: On Windows install the appropriate Microsoft ODBC Driver for SQL Server (e.g., "ODBC Driver 18 for SQL Server"). The consumer requires an ODBC driver available to `pyodbc`.

- Consumer robustness improvements
  - The `consumer.py` includes logic to recreate the `KafkaConsumer` and retry when transient errors occur.
  - `KeyboardInterrupt` is handled to close the consumer and database cleanly.

## Supplying the SQL Server connection string

- You can set the environment variable `MSSQL_CONN` before running the consumer, for example (PowerShell):

```powershell
$env:MSSQL_CONN = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=your_host,1433;DATABASE=bronze_db;UID=sa;PWD=yourpassword'
.venv\Scripts\python app\consumer\consumer.py
```

- Or run the consumer and paste the same connection string when prompted.

## Quick verification

- Start Kafka with `docker compose up -d`.
- Run `app/data_generation/producer.py` and `app/consumer/consumer.py` in separate terminals, providing a valid SQL Server connection string to the consumer.

## Next steps / enhancements

- Add automated integration tests that mock Kafka and a test SQL Server instance.
- Add optional Dockerized SQL Server service for local testing (requires large image and MS license considerations).

