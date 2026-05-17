@echo off
REM Build bdt module and run Spark Streaming app in Docker
setlocal
pushd "%~dp0bdt"

echo Building JAR...
call mvn -DskipTests clean package
if %errorlevel% neq 0 (
    echo Build failed!
    popd
    exit /b 1
)

echo Copying JAR into container...
docker cp "target\bdt-1.0-SNAPSHOT.jar" cs523bdt-lab:/opt/my_code/bdt-1.0-SNAPSHOT.jar
if %errorlevel% neq 0 (
    echo Copy failed!
    popd
    exit /b 1
)

echo Validating copied JAR inside container...
docker exec cs523bdt-lab sh -lc "jar tf /opt/my_code/bdt-1.0-SNAPSHOT.jar >/dev/null"
if %errorlevel% neq 0 (
    echo Copied JAR is invalid inside container.
    popd
    exit /b 1
)

echo Stopping any existing Spark app in container...
docker exec cs523bdt-lab sh -lc "PIDS=$(pgrep -f 'com.cs523.Main|bdt-1.0-SNAPSHOT.jar' || true); if [ -n \"$PIDS\" ]; then kill -TERM $PIDS; sleep 2; fi; PIDS=$(pgrep -f 'com.cs523.Main|bdt-1.0-SNAPSHOT.jar' || true); if [ -n \"$PIDS\" ]; then kill -KILL $PIDS; fi"

echo Ensuring Hive Metastore process is running in container...
docker exec cs523bdt-lab sh -lc "if ! pgrep -f 'HiveMetaStore|hive --service metastore' >/dev/null; then nohup hive --service metastore -p 9083 >/tmp/metastore.log 2>&1 & fi"

echo Waiting for Hive Metastore on localhost:9083 inside container...
set /a attempts=0
:wait_metastore
docker exec cs523bdt-lab sh -lc "netstat -tln 2>/dev/null | grep -q ':9083'"
if %errorlevel% equ 0 goto metastore_ready
set /a attempts+=1
if %attempts% equ 30 (
    echo Hive Metastore not ready yet. Retrying metastore startup once...
    docker exec cs523bdt-lab sh -lc "pkill -f 'HiveMetaStore|hive --service metastore' >/dev/null 2>&1 || true; nohup hive --service metastore -p 9083 >/tmp/metastore.log 2>&1 &"
)
if %attempts% geq 90 (
    echo Hive Metastore did not become ready after 180 seconds.
    popd
    exit /b 1
)
timeout /t 2 /nobreak >nul
goto wait_metastore

:metastore_ready
echo Hive Metastore is ready.

echo Running Spark Streaming app...
docker exec cs523bdt-lab sh -lc "/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --class com.cs523.Main /opt/my_code/bdt-1.0-SNAPSHOT.jar kafka-server:9092 thrift://localhost:9083"
if %errorlevel% neq 0 (
    echo Spark app failed!
    popd
    exit /b 1
)

popd
echo Done!