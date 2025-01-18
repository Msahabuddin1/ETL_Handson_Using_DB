-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE yellowTrip_bronze
AS SELECT current_timestamp() receipt_time, input_file_name() source_file, *
FROM cloud_files("dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata", "csv",
map("cloudFiles.schemaHints", "passenger_count INTEGER,trip_distance DOUBLE, total_amount
DOUBLE","header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE taxi_zones
AS SELECT * FROM cloud_files("dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellow_green_commondata/taxizones", "csv",
map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

---Declare Silver Tables

CREATE OR REFRESH STREAMING LIVE TABLE yelloTrip_taxiZones_Silver
(CONSTRAINT positive_passenger_count EXPECT (passenger_count > 0) ON VIOLATION DROP ROW,
CONSTRAINT positive_trip_distance EXPECT (trip_distance > 0.0) ON VIOLATION DROP ROW,
CONSTRAINT positive_total_amount EXPECT (total_amount > 0.0) ON VIOLATION DROP ROW,
CONSTRAINT pickup_drop_datetime_not_same EXPECT (pickup_datetime!=dropoff_datetime ) ON
VIOLATION DROP ROW
)
AS SELECT
a.VendorID,
a.tpep_pickup_datetime pickup_datetime,
a.tpep_dropoff_datetime dropoff_datetime,
CAST(a.passenger_count AS INTEGER) passenger_count,
CAST(a.trip_distance AS DOUBLE) trip_distance,
a.DOLocationID,
CAST(a.total_amount AS DOUBLE) total_amount,
b.Borough,
b.Zone,
b.service_zone
FROM STREAM(live.yellowTrip_bronze) a
INNER JOIN STREAM(live.taxi_zones) b
ON a.DOLocationID = b.LocationID

-- COMMAND ----------

---Declare Gold Table

CREATE OR REFRESH LIVE TABLE vendor_zone_passenger_count
COMMENT "Total Passenger count for each yellow taxi vendor for each zone"
AS SELECT VendorID, Zone, sum(passenger_count) total_passenger_count
FROM live.yelloTrip_taxiZones_Silver
GROUP BY VendorID, Zone
