#!/bin/bash

# Kafka Electric Vehicle Data Pipeline - Unix/Linux/Mac Script

set -u

interactive_mode() {
    echo "Interactive Mode"
    echo ""
    echo "Before proceeding, ensure:"
    echo "1. Kafka is running on localhost:9092"
    echo "2. CSV files are downloaded (Electric_Vehicle_Location_Data.csv and Electric_Vehicle_Data.csv)"
    echo "3. App is packaged (run: ./run.sh build)"
    echo ""
    echo "Options:"
    echo "   1 - Build project"
    echo "   2 - Run Producer (single CSV to one topic)"
    echo "   3 - Run Producer (both CSVs to two topics)"
    echo "   4 - Run Consumer"
    echo "   5 - Delete Topics Before Rerun"
    echo "   6 - Reset Consumer Offset to Earliest"
    echo "   7 - Exit"
    echo ""
    read -r -p "Select option (1-7): " choice

    case "$choice" in
        1) build ;;
        2) producer ;;
        3) producer_both ;;
        4) consumer ;;
        5) reset ;;
        6) reset_offset ;;
        7) echo "Exiting..."; exit 0 ;;
        *) echo "Invalid option"; interactive_mode ;;
    esac
}

command_mode() {
    case "$1" in
        build)
            build
            ;;
        produce)
            csv_file="${2:-Electric_Vehicle_Data.csv}"
            produce_max_msgs="${3:-0}"
            produce_with_file "$csv_file" "$produce_max_msgs"
            ;;
        produce-both)
            location_csv="${2:-Electric_Vehicle_Location_Data.csv}"
            evdata_csv="${3:-Electric_Vehicle_Data.csv}"
            produce_both_max_msgs="${4:-0}"
            produce_both_with_files "$location_csv" "$evdata_csv" "$produce_both_max_msgs"
            ;;
        consume)
            max_msgs="${2:-0}"
            consume_with_limit "$max_msgs"
            ;;
        reset)
            reset
            ;;
        reset-offset)
            reset_group="${2:-ev-consumer-group}"
            reset_topic="${3:-both}"

            case "$reset_topic" in
                1|location)
                    reset_topic="electric-vehicle-location"
                    ;;
                2|evdata)
                    reset_topic="electric-vehicle-evdata"
                    ;;
                both)
                    reset_offset_execute_both
                    return
                    ;;
            esac
            reset_offset_execute
            ;;
        *)
            echo "Unknown command: $1"
            show_usage
            exit 1
            ;;
    esac
}

build() {
    echo ""
    echo "Building project..."
    echo ""
    mvn -f bdt/pom.xml clean package -DskipTests
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
    echo "Build successful!"
}

producer() {
    read -r -p "Enter CSV file path (default: Electric_Vehicle_Data.csv): " csv_file
    csv_file="${csv_file:-Electric_Vehicle_Data.csv}"
    read -r -p "Enter max messages to produce (0 for all): " produce_max_msgs
    produce_max_msgs="${produce_max_msgs:-0}"
    produce_with_file "$csv_file" "$produce_max_msgs"
}

producer_both() {
    read -r -p "Enter location CSV path (default: Electric_Vehicle_Location_Data.csv): " location_csv
    location_csv="${location_csv:-Electric_Vehicle_Location_Data.csv}"
    read -r -p "Enter EVData CSV path (default: Electric_Vehicle_Data.csv): " evdata_csv
    evdata_csv="${evdata_csv:-Electric_Vehicle_Data.csv}"
    read -r -p "Enter max messages per topic (0 for all): " produce_both_max_msgs
    produce_both_max_msgs="${produce_both_max_msgs:-0}"
    produce_both_with_files "$location_csv" "$evdata_csv" "$produce_both_max_msgs"
}

produce_with_file() {
    local csv_file="$1"
    local produce_max_msgs="$2"

    echo ""
    echo "Starting Producer..."
    echo "CSV File: $csv_file"
    echo "Max Messages: $produce_max_msgs"
    echo ""

    if [ "$produce_max_msgs" = "0" ]; then
        java -jar bdt/target/bdt-fat.jar produce "$csv_file"
    else
        java -jar bdt/target/bdt-fat.jar produce "$csv_file" "$produce_max_msgs"
    fi
}

produce_both_with_files() {
    local location_csv="$1"
    local evdata_csv="$2"
    local produce_both_max_msgs="$3"

    echo ""
    echo "Starting Producer (Both Datasets)..."
    echo "Location CSV: $location_csv"
    echo "EVData CSV: $evdata_csv"
    echo "Max Messages Per Topic: $produce_both_max_msgs"
    echo ""

    if [ "$produce_both_max_msgs" = "0" ]; then
        java -jar bdt/target/bdt-fat.jar produce-both "$location_csv" "$evdata_csv"
    else
        java -jar bdt/target/bdt-fat.jar produce-both "$location_csv" "$evdata_csv" "$produce_both_max_msgs"
    fi
}

consumer() {
    read -r -p "Enter max messages to consume (0 for unlimited): " max_msgs
    max_msgs="${max_msgs:-0}"
    consume_with_limit "$max_msgs"
}

consume_with_limit() {
    local max_msgs="$1"

    echo ""
    echo "Starting Consumer..."
    echo "Max Messages: $max_msgs"
    if [ "$max_msgs" = "0" ]; then
        read -r -p "Auto-reset BOTH topic offsets before unlimited consume? (Y/n): " reset_before_unlimited
        reset_before_unlimited="${reset_before_unlimited:-Y}"

        if [ "$reset_before_unlimited" = "Y" ] || [ "$reset_before_unlimited" = "y" ]; then
            reset_group="ev-consumer-group"
            reset_topic="electric-vehicle-location"
            echo "Resetting offsets to earliest before unlimited consume..."
            do_reset_offset || exit 1
            reset_topic="electric-vehicle-evdata"
            do_reset_offset || exit 1
            echo ""
            echo "Offset reset completed for both topics."
        else
            echo "Skipping auto-reset. Using current offsets."
        fi

        echo "Auto-stops after a few seconds with no new records. Press Ctrl+C to stop immediately."
    else
        echo "Auto-stops after a few seconds when no more records are available, or when the limit is reached. Press Ctrl+C to stop immediately."
    fi
    echo ""

    if [ "$max_msgs" = "0" ]; then
        java -jar bdt/target/bdt-fat.jar consume
    else
        java -jar bdt/target/bdt-fat.jar consume "$max_msgs"
    fi
}

reset() {
    echo ""
    echo "Resetting topic before rerun..."
    echo ""
    java -jar bdt/target/bdt-fat.jar reset
}

reset_offset() {
    read -r -p "Enter consumer group (default: ev-consumer-group): " reset_group
    reset_group="${reset_group:-ev-consumer-group}"
    echo "Enter topic selection (1=location, 2=evdata, default=both):"
    read -r -p "Selection: " reset_topic_choice

    case "$reset_topic_choice" in
        "")
            reset_offset_execute_both
            ;;
        1)
            reset_topic="electric-vehicle-location"
            reset_offset_execute
            ;;
        2)
            reset_topic="electric-vehicle-evdata"
            reset_offset_execute
            ;;
        *)
            echo "Invalid selection. Use 1, 2, or press Enter for both."
            reset_offset
            ;;
    esac
}

reset_offset_execute_both() {
    reset_topic="electric-vehicle-location"
    do_reset_offset || exit 1
    reset_topic="electric-vehicle-evdata"
    do_reset_offset || exit 1
    echo ""
    echo "Offset reset completed for both topics."
}

reset_offset_execute() {
    do_reset_offset || exit 1
    echo ""
    echo "Offset reset completed."
}

do_reset_offset() {
    local reset_attempt=0
    local reset_max_attempts=5
    local reset_log

    echo ""
    echo "Resetting offsets to earliest..."
    echo "Group: $reset_group"
    echo "Topic: $reset_topic"
    echo ""

    while [ "$reset_attempt" -lt "$reset_max_attempts" ]; do
        reset_attempt=$((reset_attempt + 1))
        echo "Attempt $reset_attempt of $reset_max_attempts..."

        reset_log="$(mktemp)"
        docker exec kafka-server sh -c "kafka-consumer-groups --bootstrap-server localhost:9092 --group $reset_group --topic $reset_topic --reset-offsets --to-earliest --execute" > "$reset_log" 2>&1
        reset_cmd_exit=$?
        cat "$reset_log"

        if grep -q "UnknownTopicOrPartitionException" "$reset_log"; then
            rm -f "$reset_log"
            echo ""
            echo "Topic does not exist yet, skipping offset reset."
            return 0
        fi

        if grep -q "Error:" "$reset_log"; then
            if grep -q "current state is Stable\|current state is CompletingRebalance" "$reset_log" && [ "$reset_attempt" -lt "$reset_max_attempts" ]; then
                rm -f "$reset_log"
                echo ""
                echo "Consumer group is active/rebalancing. Waiting 3 seconds before retry..."
                sleep 3
                continue
            fi

            rm -f "$reset_log"
            echo ""
            echo "Offset reset failed. The consumer group may still be active or rebalancing."
            echo "Stop active consumers and retry. You can check with:"
            echo "  docker exec kafka-server sh -c 'kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $reset_group'"
            return 1
        fi

        rm -f "$reset_log"

        if [ "$reset_cmd_exit" -ne 0 ]; then
            echo ""
            echo "Offset reset failed. Ensure the consumer group is inactive, then retry."
            return 1
        fi

        return 0
    done

    echo ""
    echo "Offset reset failed after $reset_max_attempts attempts."
    return 1
}

show_usage() {
    echo ""
    echo "Usage:"
    echo "   ./run.sh                           - Interactive mode"
    echo "   ./run.sh build                     - Build project"
    echo "   ./run.sh produce [csv-file] [max-messages] - Run producer"
    echo "   ./run.sh produce-both [location-csv] [evdata-csv] [max-messages-per-topic] - Run producer for both datasets"
    echo "   ./run.sh consume [max-messages]    - Run consumer"
    echo "   ./run.sh reset                     - Delete and recreate topic before rerun"
    echo "   ./run.sh reset-offset [group] [topic] - Topic supports 1|2|location|evdata|both (default: both)"
    echo ""
}

echo ""
echo "========================================"
echo "Kafka Electric Vehicle Data Pipeline"
echo "========================================"
echo ""

if [ -z "${1:-}" ]; then
    interactive_mode
else
    command_mode "$@"
fi
