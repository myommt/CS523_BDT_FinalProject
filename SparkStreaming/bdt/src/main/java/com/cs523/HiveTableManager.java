package com.cs523;

import org.apache.spark.sql.SparkSession;

public final class HiveTableManager {
    private HiveTableManager() {
    }

    public static void ensureTables(SparkSession spark, AppConfig config) {
        String baseLocation = config.warehousePath() + "/" + config.databaseName() + ".db";

        spark.sql("CREATE DATABASE IF NOT EXISTS " + config.databaseName());

        // Location data table
        spark.sql(
            "CREATE EXTERNAL TABLE IF NOT EXISTS " + config.fullyQualifiedLocationTable() + " (" +
                "dol_vehicle_id STRING, " +
                "vin STRING, " +
                "county STRING, " +
                "city STRING, " +
                "state STRING, " +
                "postal_code STRING, " +
                "vehicle_location STRING" +
                ") STORED AS PARQUET LOCATION '" + baseLocation + "/ev_location_data'"
        );

        // Evdata table
        spark.sql(
            "CREATE EXTERNAL TABLE IF NOT EXISTS " + config.fullyQualifiedEvdataTable() + " (" +
                "dol_vehicle_id STRING, " +
                "vin STRING, " +
                "model_year INT, " +
                "make STRING, " +
                "model STRING, " +
                "electric_vehicle_type STRING, " +
                "cafv_eligibility STRING, " +
                "electric_range INT, " +
                "legislative_district STRING, " +
                "electric_utility STRING, " +
                "census_tract_2020 STRING" +
                ") STORED AS PARQUET LOCATION '" + baseLocation + "/ev_data'"
        );

        // Aggregated results per batch from joined streams. 
        spark.sql("DROP TABLE IF EXISTS " + config.fullyQualifiedEvaggregationTable());
        spark.sql(
            "CREATE EXTERNAL TABLE " + config.fullyQualifiedEvaggregationTable() + " (" +
                "county STRING, " +
                "make STRING, " +
                "vehicle_count BIGINT, " +
                "avg_electric_range DOUBLE" +
                ") STORED AS PARQUET LOCATION '" + baseLocation + "/ev_aggregation'"
        );
    }
}


