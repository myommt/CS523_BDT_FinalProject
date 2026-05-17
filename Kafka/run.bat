@echo off
REM Kafka Electric Vehicle Data Pipeline - Windows Script

setlocal EnableExtensions EnableDelayedExpansion

echo.
echo ========================================
echo Kafka Electric Vehicle Data Pipeline
echo ========================================
echo.

if "%~1"=="" goto interactive_mode

if /I "%~1"=="build" goto build
if /I "%~1"=="produce" goto produce_cmd
if /I "%~1"=="produce-both" goto produce_both_cmd
if /I "%~1"=="consume" goto consume_cmd
if /I "%~1"=="reset" goto reset_cmd
if /I "%~1"=="reset-offset" goto reset_offset_cmd

echo Unknown command: %~1
goto show_usage

:interactive_mode
echo Interactive Mode
echo.
echo Before proceeding, ensure:
echo 1. Kafka is running on localhost:9092
echo 2. CSV files are downloaded (Electric_Vehicle_Location_Data.csv and Electric_Vehicle_Data.csv)
echo 3. App is packaged (run: run.bat build)
echo.
echo Options:
echo   1 - Build project
echo   2 - Run Producer (both CSVs to two topics)
echo   3 - Run Consumer
echo   4 - Delete Topics Before Rerun
echo   5 - Reset Consumer Offset to Earliest
echo   6 - Exit
echo.
set /p choice="Select option (1-6): "

if "%choice%"=="1" goto build
if "%choice%"=="2" goto producer_both
if "%choice%"=="3" goto consumer
if "%choice%"=="4" goto reset
if "%choice%"=="5" goto reset_offset
if "%choice%"=="6" (
    echo Exiting...
    goto end
)

echo Invalid option
goto interactive_mode

:produce_cmd
set "csv_file=%~2"
if "%csv_file%"=="" set "csv_file=Electric_Vehicle_Data.csv"
set "produce_max_msgs=%~3"
if "%produce_max_msgs%"=="" set "produce_max_msgs=0"
goto produce_with_file

:produce_both_cmd
set "location_csv=%~2"
if "%location_csv%"=="" set "location_csv=Electric_Vehicle_Location_Data.csv"
set "evdata_csv=%~3"
if "%evdata_csv%"=="" set "evdata_csv=Electric_Vehicle_Data.csv"
set "produce_both_max_msgs=%~4"
if "%produce_both_max_msgs%"=="" set "produce_both_max_msgs=0"
goto produce_both_with_files

:consume_cmd
set "max_msgs=%~2"
if "%max_msgs%"=="" set "max_msgs=0"
goto consume_with_limit

:reset_cmd
goto reset

:reset_offset_cmd
set "reset_group=%~2"
if "%reset_group%"=="" set "reset_group=ev-consumer-group"
set "reset_topic=%~3"
if "%reset_topic%"=="" set "reset_topic=both"

if /I "%reset_topic%"=="1" set "reset_topic=electric-vehicle-location"
if /I "%reset_topic%"=="2" set "reset_topic=electric-vehicle-evdata"
if /I "%reset_topic%"=="location" set "reset_topic=electric-vehicle-location"
if /I "%reset_topic%"=="evdata" set "reset_topic=electric-vehicle-evdata"

if /I "%reset_topic%"=="both" goto reset_offset_execute_both
goto reset_offset_execute

:build
echo.
echo Building project...
echo.
mvn -f bdt\pom.xml clean package -DskipTests
if errorlevel 1 (
    echo Build failed!
    goto end_error
)
echo Build successful!
goto end

:producer
set /p csv_file="Enter CSV file path (default: Electric_Vehicle_Data.csv): "
if "%csv_file%"=="" set "csv_file=Electric_Vehicle_Data.csv"
set /p produce_max_msgs="Enter max messages to produce (0 for all): "
if "%produce_max_msgs%"=="" set "produce_max_msgs=0"
goto produce_with_file

:producer_both
set /p location_csv="Enter location CSV path (default: Electric_Vehicle_Location_Data.csv): "
if "%location_csv%"=="" set "location_csv=Electric_Vehicle_Location_Data.csv"
set /p evdata_csv="Enter EVData CSV path (default: Electric_Vehicle_Data.csv): "
if "%evdata_csv%"=="" set "evdata_csv=Electric_Vehicle_Data.csv"
set /p produce_both_max_msgs="Enter max messages per topic (0 for all): "
if "%produce_both_max_msgs%"=="" set "produce_both_max_msgs=0"
goto produce_both_with_files

:produce_with_file
echo.
echo Starting Producer...
echo CSV File: %csv_file%
echo Max Messages: %produce_max_msgs%
echo.
if "%produce_max_msgs%"=="0" (
    java -jar bdt\target\bdt-fat.jar produce "%csv_file%"
) else (
    java -jar bdt\target\bdt-fat.jar produce "%csv_file%" %produce_max_msgs%
)
goto end

:produce_both_with_files
echo.
echo Starting Producer (Both Datasets)...
echo Location CSV: %location_csv%
echo EVData CSV: %evdata_csv%
echo Max Messages Per Topic: %produce_both_max_msgs%
echo.
if "%produce_both_max_msgs%"=="0" (
    java -jar bdt\target\bdt-fat.jar produce-both "%location_csv%" "%evdata_csv%"
) else (
    java -jar bdt\target\bdt-fat.jar produce-both "%location_csv%" "%evdata_csv%" %produce_both_max_msgs%
)
goto end

:consumer
set /p max_msgs="Enter max messages to consume (0 for unlimited): "
if "%max_msgs%"=="" set "max_msgs=0"
goto consume_with_limit

:consume_with_limit
echo.
echo Starting Consumer...
echo Max Messages: %max_msgs%
if "%max_msgs%"=="0" (
    set /p reset_before_unlimited="Auto-reset BOTH topic offsets before unlimited consume? (Y/n): "
    if /I "%reset_before_unlimited%"=="" set "reset_before_unlimited=Y"
    if /I "%reset_before_unlimited%"=="Y" (
        set "reset_group=ev-consumer-group"
        set "reset_topic=electric-vehicle-location"
        echo Resetting offsets to earliest before unlimited consume...
        call :do_reset_offset
        if errorlevel 1 goto end_error
        set "reset_topic=electric-vehicle-evdata"
        call :do_reset_offset
        if errorlevel 1 goto end_error
        echo.
        echo Offset reset completed for both topics.
    ) else (
        echo Skipping auto-reset. Using current offsets.
    )
    echo Auto-stops after a few seconds with no new records. Press Ctrl+C to stop immediately.
) else (
    echo Auto-stops after a few seconds when no more records are available, or when the limit is reached. Press Ctrl+C to stop immediately.
)
echo.
if "%max_msgs%"=="0" (
    java -jar bdt\target\bdt-fat.jar consume
) else (
    java -jar bdt\target\bdt-fat.jar consume %max_msgs%
)
goto end

:reset
echo.
echo Resetting topic before rerun...
echo.
java -jar bdt\target\bdt-fat.jar reset
goto end

:reset_offset
set /p reset_group="Enter consumer group (default: ev-consumer-group): "
if "%reset_group%"=="" set "reset_group=ev-consumer-group"
echo Enter topic selection (1=location, 2=evdata, default=both):
set /p reset_topic_choice="Selection: "
if "%reset_topic_choice%"=="" goto reset_offset_execute_both
if "%reset_topic_choice%"=="1" (
    set "reset_topic=electric-vehicle-location"
    goto reset_offset_execute
)
if "%reset_topic_choice%"=="2" (
    set "reset_topic=electric-vehicle-evdata"
    goto reset_offset_execute
)
echo Invalid selection. Use 1, 2, or press Enter for both.
goto reset_offset

:reset_offset_execute_both
set "reset_topic=electric-vehicle-location"
call :do_reset_offset
if errorlevel 1 goto end_error
set "reset_topic=electric-vehicle-evdata"
call :do_reset_offset
if errorlevel 1 goto end_error

echo.
echo Offset reset completed for both topics.
goto end

:reset_offset_execute
call :do_reset_offset
if errorlevel 1 goto end_error

echo.
echo Offset reset completed.
goto end

:do_reset_offset
echo.
echo Resetting offsets to earliest...
echo Group: %reset_group%
echo Topic: %reset_topic%
echo.
:set_reset_attempt
set /a reset_attempt=0
set /a reset_max_attempts=5

:reset_offset_retry
set /a reset_attempt+=1
echo Attempt !reset_attempt! of !reset_max_attempts!...
set "reset_log=%TEMP%\kafka_reset_offset_%RANDOM%.log"
docker exec kafka-server sh -c "kafka-consumer-groups --bootstrap-server localhost:9092 --group %reset_group% --topic %reset_topic% --reset-offsets --to-earliest --execute" > "%reset_log%" 2>&1
set "reset_cmd_exit=%ERRORLEVEL%"
type "%reset_log%"
findstr /C:"Error:" "%reset_log%" >nul
set "has_error_text=%ERRORLEVEL%"
findstr /C:"UnknownTopicOrPartitionException" "%reset_log%" >nul
set "is_unknown_topic=%ERRORLEVEL%"
findstr /C:"current state is Stable" /C:"current state is CompletingRebalance" "%reset_log%" >nul
set "is_group_active=%ERRORLEVEL%"
del "%reset_log%" >nul 2>&1

if "%is_unknown_topic%"=="0" (
    echo.
    echo Topic does not exist yet, skipping offset reset.
    exit /b 0
)

if not "%reset_cmd_exit%"=="0" (
    echo.
    echo Offset reset failed. Ensure the consumer group is inactive, then retry.
    exit /b 1
)

if "%has_error_text%"=="0" (
    if "%is_group_active%"=="0" (
        if !reset_attempt! LSS !reset_max_attempts! (
            echo.
            echo Consumer group is active/rebalancing. Waiting 3 seconds before retry...
            timeout /t 3 /nobreak >nul
            goto reset_offset_retry
        )
    )

    echo.
    echo Offset reset failed. The consumer group may still be active or rebalancing.
    echo Stop active consumers and retry. You can check with:
    echo   docker exec kafka-server sh -c "kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group %reset_group%"
    exit /b 1
)

exit /b 0

:show_usage
echo.
echo Usage:
echo   run.bat                           - Interactive mode
echo   run.bat build                     - Build project
echo   run.bat produce [csv-file] [max-messages] - Run producer
echo   run.bat produce-both [location-csv] [evdata-csv] [max-messages-per-topic] - Run producer for both datasets
echo   run.bat consume [max-messages]    - Run consumer
echo   run.bat reset                     - Delete and recreate topic before rerun
echo   run.bat reset-offset [group] [topic] - Topic supports 1^|2^|location^|evdata^|both (default: both)
echo.
goto end_error

:end
endlocal
exit /b 0

:end_error
endlocal
exit /b 1
