package com.cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.StructType;

public final class EVStreamTransformations {
    private EVStreamTransformations() {
    }

    /**
     * Parse and validate location topic data in one pass
     * Returns only valid records
     */
    public static Dataset<Row> getValidLocationData(SparkSession spark, AppConfig config) {
        StructType schema = EVSchemaFactory.locationSchema();
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.bootstrapServers())
                .option("subscribe", config.locationTopic())
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(key AS STRING) AS dol_vehicle_id", "CAST(value AS STRING) AS json_payload", "timestamp AS event_time")
                .select(
                        col("dol_vehicle_id"),
                        col("event_time"),
                        from_json(col("json_payload"), schema).alias("ev")
                )
                .select(
                        col("dol_vehicle_id"),
                        col("event_time"),
                        col("ev.`vin_(1-10)`").alias("vin"),
                        col("ev.county").alias("county"),
                        col("ev.city").alias("city"),
                        col("ev.state").alias("state"),
                        col("ev.postal_code").alias("postal_code"),
                        col("ev.vehicle_location").alias("vehicle_location")
                )
                .filter(col("dol_vehicle_id").isNotNull());
    }

    /**
     * Parse and validate evdata topic data in one pass
     * Returns only valid records: electric_range NOT NULL AND >= 0 AND <= 600 AND model_year >= 1990 AND <= 2025
     */
    public static Dataset<Row> getValidEvdataData(SparkSession spark, AppConfig config) {
        StructType schema = EVSchemaFactory.evdataSchema();
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.bootstrapServers())
                .option("subscribe", config.evdataTopic())
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(key AS STRING) AS dol_vehicle_id", "CAST(value AS STRING) AS json_payload", "timestamp AS event_time")
                .select(
                        col("dol_vehicle_id"),
                        col("event_time"),
                        from_json(col("json_payload"), schema).alias("ev")
                )
                .select(
                        col("dol_vehicle_id"),
                        col("event_time"),
                        col("ev.`vin_(1-10)`").alias("vin"),
                        col("ev.model_year").cast("int").alias("model_year"),
                        col("ev.make").alias("make"),
                        col("ev.model").alias("model"),
                        col("ev.electric_vehicle_type").alias("electric_vehicle_type"),
                        col("ev.`clean_alternative_fuel_vehicle_(cafv)_eligibility`").alias("cafv_eligibility"),
                        col("ev.electric_range").cast("int").alias("electric_range"),
                        col("ev.legislative_district").alias("legislative_district"),
                        col("ev.electric_utility").alias("electric_utility"),
                        col("ev.`2020_census_tract`").alias("census_tract_2020")
                )
                .filter(col("electric_range").isNotNull()
                        .and(col("electric_range").geq(0))
                        .and(col("electric_range").leq(600))
                        .and(col("model_year").geq(1990))
                        .and(col("model_year").leq(2025)));
    }

        /**
         * Build joined stream from location + evdata on dol_vehicle_id.
         * Output columns are used for later batch aggregation.
         */
    public static Dataset<Row> buildEvaggregation(Dataset<Row> validLocation, Dataset<Row> validEvdata) {
        // Add watermarks so Spark can bound state for stream-stream join
        Dataset<Row> locStream = validLocation.withWatermark("event_time", "10 minutes").alias("l");
        Dataset<Row> evStream = validEvdata.withWatermark("event_time", "10 minutes").alias("e");

        // Bound join by key + event-time range so state can be evicted.
        return locStream.join(
                        evStream,
                        col("l.dol_vehicle_id").equalTo(col("e.dol_vehicle_id"))
                                .and(col("l.event_time").geq(col("e.event_time").minus(org.apache.spark.sql.functions.expr("INTERVAL 10 MINUTES"))))
                                .and(col("l.event_time").leq(col("e.event_time").plus(org.apache.spark.sql.functions.expr("INTERVAL 10 MINUTES"))))
                )
                .select(
                        col("l.dol_vehicle_id").alias("dol_vehicle_id"),
                        col("l.event_time").alias("event_time"),
                        col("l.county").alias("county"),
                        col("e.make").alias("make"),
                        col("e.electric_range").alias("electric_range")
                );
    }

    /**
     * SQL equivalent:
     * SELECT county, make, COUNT(1) AS vehicle_count, AVG(electric_range) AS avg_electric_range
     * FROM joined_data
     * GROUP BY county, make
     */
        public static Dataset<Row> buildEvaggregation(Dataset<Row> batchData) {
        // Deduplicate by dol_vehicle_id first to avoid inflated counts from stream-stream join duplicates
        Dataset<Row> deduped = batchData.dropDuplicates(new String[]{"dol_vehicle_id"});
        return deduped
                .groupBy(col("county"), col("make"))
                .agg(
                        count(lit(1)).alias("vehicle_count"),
                        avg(col("electric_range")).alias("avg_electric_range")
                                );
    }
}
