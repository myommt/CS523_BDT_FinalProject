# Spark Structured Streaming to Hive (EV Data)

This project consumes EV JSON messages from two separate Kafka topics, performs real-time filtering for data quality, and writes valid processed output to Hive tables.

## What this app does

- Subscribes to two Kafka topics: `electric-vehicle-location`, `electric-vehicle-evdata`
- Filters data quality in-stream before writing to Hive:
  - **Location**: Only writes records with non-null county and vehicle_location
  - **Evdata**: Only writes records with valid electric_range (0-600) and model_year (1990-2025)
- Computes moving averages and aggregations:
  - 5-minute window, sliding every 1 minute
  - grouped by `county` and `make`
  - metrics: `vehicle_count`, `avg_electric_range`
- Persists three separate outputs to Hive tables:
  1. `ev_db.ev_location_data` — Valid location records
  2. `ev_db.ev_data` — Valid evdata records
  3. `ev_db.ev_aggregation` — Windowed aggregations (count + avg_electric_range by county, make)

## Project entry point

- Main class: `com.cs523.Main`

## Project structure

```text
bdt/
  pom.xml
  README.md
  src/
    main/
      java/
        com/
          cs523/
            Main.java
            AppConfig.java
            EVStreamingToHiveApp.java
            EVSchemaFactory.java
            EVStreamTransformations.java
            HiveTableManager.java
            HiveStreamWriters.java
```

## Code description

### `Main.java`

- Thin entry point.
- Builds configuration from command-line args and starts the app.

### `AppConfig.java`

- Central runtime configuration model.
- Holds Kafka bootstrap server, Hive metastore, warehouse, database/table names, and checkpoint paths.
- Provides methods to get fully qualified table names and topic names.

### `EVStreamingToHiveApp.java`

- Application orchestrator.
- Creates `SparkSession` with Hive support.
- Orchestrates three streaming pipelines:
  1. **Location pipeline**: Read location topic → parse + filter valid → write
  2. **Evdata pipeline**: Read evdata topic → parse + filter valid → write
  3. **Aggregation**: Join valid location + evdata → window → aggregate
- Starts three concurrent streaming writers.
- Keeps streaming job alive via `awaitAnyTermination()`.

### `EVSchemaFactory.java`

- Defines the JSON schema for incoming Kafka EV messages.
- Keeps schema changes isolated in one place.

### `EVStreamTransformations.java`

- **Location processing** (single-pass):
  - `getValidLocationData()` — Reads location topic, parses JSON, filters for non-null county and vehicle_location
- **Evdata processing** (single-pass):
  - `getValidEvdataData()` — Reads evdata topic, parses JSON, filters for valid electric_range (0-600) and model_year (1990-2025)
- **Aggregation**:
  - `buildMovingAverages()` — Joins valid location + evdata on dol_vehicle_id, groups by window + county + make, computes vehicle_count and avg_electric_range

### `HiveTableManager.java`

- Creates Hive database and three tables if they do not exist.
- Table schemas:
  - `ev_location_data`: dol_vehicle_id, event_time, vin, county, city, state, postal_code, vehicle_location
  - `ev_data`: dol_vehicle_id, event_time, vin, model_year, make, model, electric_vehicle_type, cafv_eligibility, electric_range, legislative_district, electric_utility, census_tract_2020
  - `ev_aggregation`: county, make, vehicle_count, avg_electric_range

### `HiveStreamWriters.java`

- Starts three structured streaming sinks using `foreachBatch`:
  - `startLocationDataWriter()` — Writes valid location records to ev_location_data
  - `startEvdataWriter()` — Writes valid evdata records to ev_data
  - `startMovingAveragesWriter()` — Writes windowed aggregations to ev_aggregation
- Each writer has its own checkpoint for independent offset tracking.
- All writers use append mode (immutable stream semantics).

## Data flow overview

1. Two Kafka topics (`electric-vehicle-location` and `electric-vehicle-evdata`) provide JSON records.
2. Spark reads each topic independently using the Kafka message key (dol_vehicle_id) for correlation.
3. Location stream is parsed and **filtered for valid records** (non-null county, vehicle_location) — invalid records are dropped.
4. Evdata stream is parsed and **filtered for valid records** (valid electric_range and model_year) — invalid records are dropped.
5. Valid location and evdata records are joined on dol_vehicle_id.
6. Joined stream is windowed (5-min tumbling, 1-min slide) and aggregated by county + make.
7. All three streams (location, evdata, moving_averages) write independently to Hive in parallel.
8. Each stream maintains its own checkpoint for restart safety and offset tracking.

## Prerequisites

- Java 17 installed
- Maven installed
- Kafka broker running and topic already populated with data
- Hive Metastore Thrift endpoint running (commonly `thrift://localhost:9083`)

## Quick run (Windows PowerShell)

Run from the SparkStreaming root (the parent folder of `bdt`):

```powershell
cd <path-to-SparkStreaming>
.\run.bat
```

This script will:
1. Build the JAR with Maven
2. Copy it into the running container using `docker cp`
3. Execute the Spark job inside the Docker container (connecting to Kafka and Hive)
4. Stop any previous Spark app instance in the container before starting (prevents checkpoint collisions)

The script requires Docker containers to be running (see setup section below).

## Setup with Docker (one-time setup required before run.bat)

Before you can use `run.bat`, you must configure and start the Docker containers.

1. Ensure `docker-compose.yml` exposes ports `9083` (Hive Metastore Thrift) and `10000` (HiveServer2):

```yaml
ports:
  - "9083:9083"   # Hive Metastore Thrift
  - "10000:10000" # Hive JDBC (HiveServer2)
```

2. Start the containers:

```powershell
docker compose -f ..\cs523-bdt\docker-compose.yml down
docker compose -f ..\cs523-bdt\docker-compose.yml up -d
```

3. Start the Hive metastore service inside the container:

```powershell
docker exec -d cs523bdt-lab sh -lc 'nohup hive --service metastore -p 9083 >/tmp/metastore.log 2>&1 &'
```

4. Create Hive managed table directories in HDFS (required for Spark writes):

```powershell
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse/ev_db.db/ev_location_data
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse/ev_db.db/ev_data
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse/ev_db.db/ev_aggregation
docker exec cs523bdt-lab hdfs dfs -chmod -R 777 /user/hive/warehouse/ev_db.db
```

**Critical:** Without these directories, Spark will fail with `Path does not exist` when attempting to write records to Hive tables. This must be done after creating containers but before running `.\run.bat`.

Once all setup steps complete, you can run `.\run.bat` from the SparkStreaming root.

## Hive Configuration

The application uses the following Hive settings:

| Setting | Value |
|---------|-------|
| **Hive Metastore URI** | `thrift://localhost:9083` |
| **Hive Warehouse Directory** | `/user/hive/warehouse` |
| **Default Database** | `ev_db` (created by app if not exists) |

These are configured in [EVStreamingToHiveApp.java](src/main/java/com/cs523/EVStreamingToHiveApp.java) when creating the SparkSession with `.enableHiveSupport()` and the `hive.metastore.uris` property.

## Test with existing Kafka data (no producer run required)

If Kafka already has data and you want to replay from beginning:

1. In `src/main/java/com/cs523/EVStreamTransformations.java`, set:

```java
.option("startingOffsets", "earliest")
```

2. Reset pipeline state using the Windows reset script:

```powershell
cd <path-to-SparkStreaming>
.\delete.bat
```

3. Start consuming again using the Windows run script:

```powershell
cd <path-to-SparkStreaming>
.\run.bat
```

## Success indicators

In app logs, look for:

- `Wrote location batch ... to ev_db.ev_location_data`
- `Wrote evdata batch ... to ev_db.ev_data`
- `Wrote Evaggregation batch ... to ev_db.ev_aggregation`

## Verify Spark → Hive connection

Before running the app, verify that Spark can reach Hive:

1. Check that metastore is running:

```powershell
docker exec cs523bdt-lab ps aux | Select-String metastore
```

Expected output should show a running `hive --service metastore` process.

2. Test connection with Hive CLI (from inside container):

```powershell
docker exec -it cs523bdt-lab hive -e "SHOW DATABASES;"
```

3. Verify warehouse directory exists:

```powershell
docker exec cs523bdt-lab hdfs dfs -ls /user/hive/warehouse
```

If warehouse doesn't exist, create it:

```powershell
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse
docker exec cs523bdt-lab hdfs dfs -chmod 777 /user/hive/warehouse
```

## Verify Hive output

After running the Spark app, use Hive CLI to query tables:

```powershell
docker exec -it cs523bdt-lab hive
```

Then in Hive CLI:

```sql
SHOW DATABASES;
USE ev_db;
SHOW TABLES;
SELECT * FROM ev_location_data ORDER BY event_time DESC LIMIT 20;
SELECT * FROM ev_data ORDER BY event_time DESC LIMIT 20;
SELECT * FROM ev_aggregation ORDER BY county LIMIT 20;
```