package com.cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EVStreamingToHiveApp {
    private final AppConfig config;

    public EVStreamingToHiveApp(AppConfig config) {
        this.config = config;
    }

    public void run() throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("EVKafkaStructuredStreamingToHive")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", config.warehousePath())
                .config("hive.metastore.uris", config.hiveMetastoreUri())
                .config("spark.hadoop.hive.metastore.uris", config.hiveMetastoreUri())
                .config("spark.hadoop.hadoop.native.lib", "false")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        HiveTableManager.ensureTables(spark, config);

        // Location topic pipeline - parse and filter in one step
        Dataset<Row> validLocationData = EVStreamTransformations.getValidLocationData(spark, config);

        // Evdata topic pipeline - parse and filter in one step
        Dataset<Row> validEvdataData = EVStreamTransformations.getValidEvdataData(spark, config);

        // Start writers for valid data; aggregation is computed inside the location writer
        HiveStreamWriters.startLocationDataWriter(validLocationData, config);
        HiveStreamWriters.startEvdataWriter(validEvdataData, config);

        spark.streams().awaitAnyTermination();
    }
}
