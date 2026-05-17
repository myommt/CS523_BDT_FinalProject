package com.cs523;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class EVSchemaFactory {
    private EVSchemaFactory() {
    }

    /**
     * Schema for electric-vehicle-location topic
     * Fields: vin, county, city, state, postal_code, dol_vehicle_id, vehicle_location
     */
    public static StructType locationSchema() {
        return new StructType()
                .add("vin_(1-10)", DataTypes.StringType)
                .add("county", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("state", DataTypes.StringType)
                .add("postal_code", DataTypes.StringType)
                .add("dol_vehicle_id", DataTypes.StringType)
                .add("vehicle_location", DataTypes.StringType);
    }

    /**
     * Schema for electric-vehicle-evdata topic
     * Fields: vin, model_year, make, model, electric_vehicle_type, cafv_eligibility, 
     *         electric_range, legislative_district, dol_vehicle_id, electric_utility, 2020_census_tract
     */
    public static StructType evdataSchema() {
        return new StructType()
                .add("vin_(1-10)", DataTypes.StringType)
                .add("model_year", DataTypes.StringType)
                .add("make", DataTypes.StringType)
                .add("model", DataTypes.StringType)
                .add("electric_vehicle_type", DataTypes.StringType)
                .add("clean_alternative_fuel_vehicle_(cafv)_eligibility", DataTypes.StringType)
                .add("electric_range", DataTypes.StringType)
                .add("legislative_district", DataTypes.StringType)
                .add("dol_vehicle_id", DataTypes.StringType)
                .add("electric_utility", DataTypes.StringType)
                .add("2020_census_tract", DataTypes.StringType);
    }
}
