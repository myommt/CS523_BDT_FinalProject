@echo off
REM Delete all data from three Hive tables and checkpoint files
REM Usage: .\delete.bat

echo.
echo =========================================
echo Cleaning up Spark Streaming Pipeline Data
echo =========================================
echo.

echo Stopping any running Spark process...
docker exec cs523bdt-lab sh -lc "pkill -f 'bdt-1.0-SNAPSHOT.jar' || true; sleep 2"
echo.

echo Deleting checkpoint files...
docker exec cs523bdt-lab hdfs dfs -rm -r -f /user/root/checkpoints/ev_location_data
docker exec cs523bdt-lab hdfs dfs -rm -r -f /user/root/checkpoints/ev_data
docker exec cs523bdt-lab hdfs dfs -rm -r -f /user/root/checkpoints/ev_aggregation
echo Checkpoint files deleted.
echo.

echo Deleting table data directories...
docker exec cs523bdt-lab hdfs dfs -rm -r -f /user/hive/warehouse/ev_db.db/ev_location_data
docker exec cs523bdt-lab hdfs dfs -rm -r -f /user/hive/warehouse/ev_db.db/ev_data
docker exec cs523bdt-lab hdfs dfs -rm -r -f /user/hive/warehouse/ev_db.db/ev_aggregation
echo Table data deleted.
echo.

echo Recreating empty table directories...
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse/ev_db.db/ev_location_data
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse/ev_db.db/ev_data
docker exec cs523bdt-lab hdfs dfs -mkdir -p /user/hive/warehouse/ev_db.db/ev_aggregation
echo Table directories recreated.
echo.

echo =========================================
echo Clean up complete!
echo.
echo To restart the pipeline, run: .\run.bat
echo =========================================
