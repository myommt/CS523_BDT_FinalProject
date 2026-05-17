# Kafka Electric Vehicle Data Pipeline

This project reads two CSV files, publishes them into Kafka, and consumes them in this order:

1. `electric-vehicle-location`
2. `electric-vehicle-evdata`

## Prerequisites

1. Java 17+
2. Maven 3.8+
3. Kafka reachable at `localhost:9092`
4. These files in project root:
   - `Electric_Vehicle_Location_Data.csv`
   - `Electric_Vehicle_Data.csv`

## Topic Names

- LOCATION: `electric-vehicle-location`
- EVDATA: `electric-vehicle-evdata`

## Fastest Way (Windows Interactive)

Run:

```bat
.\run.bat
```

1. `1` Build project
2. `4` Delete topics before rerun
3. `2` Run Producer (both CSVs)
4. `3` Run Consumer

At prompts:

- Producer:
  - Location CSV: `Electric_Vehicle_Location_Data.csv`
  - EVData CSV: `Electric_Vehicle_Data.csv`
  - Max messages per topic: `0` (all)
- Consumer:
  - Max messages: `0` for unlimited (auto-stop on idle), or `N` for bounded mode
  - If max is `0`, script asks: `Auto-reset BOTH topic offsets before unlimited consume? (Y/n)`
    - `Y` or Enter: replay both topics from earliest
    - `n`: keep current offsets

## Offset Reset (Interactive Option 5)

Option `5` now supports:

- `1` = reset LOCATION only
- `2` = reset EVDATA only
- Enter = reset BOTH topics (default)

This lets you do partial replay without touching the other topic.

## Common Workflows

### A) Full Replay of Both Topics

1. Option `5` -> press Enter (both)
2. Option `3` consume

### B) Replay Only LOCATION

1. Option `5` -> enter `1`
2. Option `3` consume
3. If consume max is `0`, answer `n` at auto-reset prompt so EVDATA offset is preserved

### C) Replay Only EVDATA

1. Option `5` -> enter `2`
2. Option `3` consume
3. If consume max is `0`, answer `n` at auto-reset prompt so LOCATION offset is preserved

## Command Mode (Windows)

Use these non-interactive commands:

```bat
run.bat build
run.bat reset
run.bat produce-both Electric_Vehicle_Location_Data.csv Electric_Vehicle_Data.csv 0
run.bat consume 0
run.bat reset-offset ev-consumer-group both
```

## macOS/Linux Script

Equivalent commands on Unix-like systems:

```bash
./run.sh build
./run.sh reset
./run.sh produce-both Electric_Vehicle_Location_Data.csv Electric_Vehicle_Data.csv 0
./run.sh consume 0
./run.sh reset-offset ev-consumer-group both
```

`reset-offset` topic values:
- `1`
- `2`
- `location`
- `evdata`
- `both`

## Direct JAR Commands

Output jar:

`bdt/target/bdt-fat.jar`

```bat
java -jar bdt\target\bdt-fat.jar produce [csv-file] [max-messages]
java -jar bdt\target\bdt-fat.jar produce [csv-file] [topic-name] [max-messages]
java -jar bdt\target\bdt-fat.jar produce-both [location-csv] [evdata-csv] [max-messages-per-topic]
java -jar bdt\target\bdt-fat.jar consume [max-messages]
java -jar bdt\target\bdt-fat.jar reset
```

## Kafka Configuration Defaults

```properties
bootstrap.servers=localhost:9092
topic.location=electric-vehicle-location
topic.evdata=electric-vehicle-evdata
csv.default.location=Electric_Vehicle_Location_Data.csv
csv.default.evdata=Electric_Vehicle_Data.csv
```

## Docker Kafka Note

If Kafka runs in Docker, expose:

- Kafka: `9092:9092`
- Zookeeper: `2181:2181` (only if your setup uses ZooKeeper)

If `localhost:9092` is reachable, this pipeline runs without code changes.
