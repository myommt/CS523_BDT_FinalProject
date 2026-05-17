package com.cs523;

public final class AppConfig {
    private final String bootstrapServers;
    private final String hiveMetastoreUri;
    private final String warehousePath;
    private final String databaseName;
    private final String locationDataCheckpoint;
    private final String evdataCheckpoint;
    private final String evaggregationCheckpoint;

    public AppConfig(
            String bootstrapServers,
            String hiveMetastoreUri,
            String warehousePath,
            String databaseName,
            String locationDataCheckpoint,
            String evdataCheckpoint,
            String evaggregationCheckpoint
    ) {
        this.bootstrapServers = bootstrapServers;
        this.hiveMetastoreUri = hiveMetastoreUri;
        this.warehousePath = warehousePath;
        this.databaseName = databaseName;
        this.locationDataCheckpoint = locationDataCheckpoint;
        this.evdataCheckpoint = evdataCheckpoint;
        this.evaggregationCheckpoint = evaggregationCheckpoint;
    }

    public static AppConfig fromArgs(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String hiveMetastoreUri = args.length > 1 ? args[1] : "thrift://localhost:9083";

        return new AppConfig(
                bootstrapServers,
                hiveMetastoreUri,
                "hdfs:///user/hive/warehouse",
                "ev_db",
                "/user/root/checkpoints/ev_location_data",
                "/user/root/checkpoints/ev_data",
                "/user/root/checkpoints/ev_aggregation"
        );
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public String hiveMetastoreUri() {
        return hiveMetastoreUri;
    }

    public String warehousePath() {
        return warehousePath;
    }

    public String databaseName() {
        return databaseName;
    }

    public String locationDataCheckpoint() {
        return locationDataCheckpoint;
    }

    public String evdataCheckpoint() {
        return evdataCheckpoint;
    }

    public String evaggregationCheckpoint() {
        return evaggregationCheckpoint;
    }

    public String locationTopic() {
        return "electric-vehicle-location";
    }

    public String evdataTopic() {
        return "electric-vehicle-evdata";
    }

    public String fullyQualifiedLocationTable() {
        return databaseName() + ".ev_location_data";
    }

    public String fullyQualifiedEvdataTable() {
        return databaseName() + ".ev_data";
    }

    public String fullyQualifiedEvaggregationTable() {
        return databaseName() + ".ev_aggregation";
    }
}
