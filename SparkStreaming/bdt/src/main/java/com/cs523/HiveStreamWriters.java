package com.cs523;

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public final class HiveStreamWriters {
    private static final Object AGGREGATION_LOCK = new Object();

    private HiveStreamWriters() {
    }

    public static StreamingQuery startLocationDataWriter(Dataset<Row> locationData, AppConfig config) throws TimeoutException {
        return locationData.writeStream()
                .outputMode("append")
                .option("checkpointLocation", config.locationDataCheckpoint())
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    synchronized (AGGREGATION_LOCK) {
                    String locationPath = config.warehousePath() + "/" + config.databaseName() + ".db/ev_location_data";

                    // Read existing data eagerly into memory to avoid file-not-found race
                    // condition when overwrite deletes files during lazy evaluation
                    List<Row> existingRows;
                    try {
                        existingRows = batchDF.sparkSession().read().parquet(locationPath).collectAsList();
                    } catch (Exception e) {
                        existingRows = java.util.Collections.emptyList();
                    }
                    Dataset<Row> existingData = batchDF.sparkSession().createDataFrame(existingRows, batchDF.schema());

                    // Union batch with existing data
                    Dataset<Row> combined = existingData.union(batchDF);

                    // Deduplicate: keep one record per dol_vehicle_id (prioritize by event_time)
                    // Sort by dol_vehicle_id, event_time DESC to ensure latest is first, then drop duplicates
                    Dataset<Row> deduped = combined
                            .orderBy(col("dol_vehicle_id"), col("event_time").desc())
                            .dropDuplicates(new String[]{"dol_vehicle_id"});

                    // Write with overwrite mode (Spark safely handles deletion during write)
                    deduped.coalesce(1).write()
                            .mode("overwrite")
                            .parquet(locationPath);
                    System.out.println("Wrote location batch " + batchId + " to " + config.fullyQualifiedLocationTable()
                            + " (deduplicated)");

                        recomputeAggregation(batchDF, config, batchId);
                        }
                })
                .start();
    }

    public static StreamingQuery startEvdataWriter(Dataset<Row> evdataData, AppConfig config) throws TimeoutException {
        return evdataData.writeStream()
                .outputMode("append")
                .option("checkpointLocation", config.evdataCheckpoint())
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    synchronized (AGGREGATION_LOCK) {
                    String evdataPath = config.warehousePath() + "/" + config.databaseName() + ".db/ev_data";
                    
                    // Read existing data eagerly into memory to avoid file-not-found race
                    // condition when overwrite deletes files during lazy evaluation
                    List<Row> existingRows;
                    try {
                        existingRows = batchDF.sparkSession().read().parquet(evdataPath).collectAsList();
                    } catch (Exception e) {
                        existingRows = java.util.Collections.emptyList();
                    }
                    Dataset<Row> existingData = batchDF.sparkSession().createDataFrame(existingRows, batchDF.schema());
                    
                    // Union batch with existing data
                    Dataset<Row> combined = existingData.union(batchDF);
                    
                    // Deduplicate: keep one record per dol_vehicle_id (prioritize by event_time)
                    // Sort by dol_vehicle_id, event_time DESC to ensure latest is first, then drop duplicates
                    Dataset<Row> deduped = combined
                            .orderBy(col("dol_vehicle_id"), col("event_time").desc())
                            .dropDuplicates(new String[]{"dol_vehicle_id"});
                    
                    // Write with overwrite mode (Spark safely handles deletion during write)
                    deduped.coalesce(1).write()
                            .mode("overwrite")
                            .parquet(evdataPath);
                    System.out.println("Wrote evdata batch " + batchId + " to " + config.fullyQualifiedEvdataTable() 
                            + " (deduplicated)");

                    recomputeAggregation(batchDF, config, batchId);
                    }
                })
                .start();
    }

    private static void recomputeAggregation(Dataset<Row> triggerBatch, AppConfig config, long batchId) {
        String locationPath = config.warehousePath() + "/" + config.databaseName() + ".db/ev_location_data";
        String evdataPath = config.warehousePath() + "/" + config.databaseName() + ".db/ev_data";
        String aggPath = config.warehousePath() + "/" + config.databaseName() + ".db/ev_aggregation";

        Dataset<Row> allLocation;
        Dataset<Row> allEvdata;
        try {
            Dataset<Row> currentLocation = triggerBatch.sparkSession().read().parquet(locationPath);
            List<Row> locationRows = currentLocation.collectAsList();
            allLocation = triggerBatch.sparkSession().createDataFrame(locationRows, currentLocation.schema());

            Dataset<Row> currentEvdata = triggerBatch.sparkSession().read().parquet(evdataPath);
            List<Row> evdataRows = currentEvdata.collectAsList();
            allEvdata = triggerBatch.sparkSession().createDataFrame(evdataRows, currentEvdata.schema());
        } catch (Exception e) {
            return;
        }

        Dataset<Row> joined = allLocation.join(allEvdata, "dol_vehicle_id");
        if (joined.isEmpty()) {
            return;
        }

        Dataset<Row> aggregated = EVStreamTransformations.buildEvaggregation(joined);

        aggregated.coalesce(1).write().mode("overwrite").parquet(aggPath);
        System.out.println("Wrote aggregation batch " + batchId + " to " + config.fullyQualifiedEvaggregationTable());
    }


}


