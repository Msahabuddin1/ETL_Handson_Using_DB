# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata/yellow_tripdata_2019_06.csv 
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaHints", "trip_distance float")
 .option("cloudFiles.schemaLocation",
f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/yellow_bronze")
 .load("dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata")
 .createOrReplaceTempView("yellowtrip_raw_temp"))


# COMMAND ----------

# MAGIC %sql
# MAGIC describe yellowtrip_raw_temp

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW yellowtrip_bronze_temp AS (
# MAGIC  SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC  FROM yellowtrip_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yellowtrip_bronze_temp

# COMMAND ----------

(spark.table("yellowtrip_bronze_temp")
 .writeStream
 .format("delta")
 .option("checkpointLocation",
f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/green_bronze")
 .outputMode("append")
 .table("yellowtrip_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yellowtrip_bronze

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yellowtrip_bronze

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/nyctaxidata/landing/yellow_green_commondata/taxizones/

# COMMAND ----------

(spark.read
 .format("csv")
 .option("header", True)
 .option("inferSchema", True)
 .load(f"dbfs:/FileStore/tables/nyctaxidata/landing/yellow_green_commondata/taxizones/TaxiZones.csv")
 .createOrReplaceTempView("taxizones_view"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxizones_view

# COMMAND ----------

# MAGIC %sql
# MAGIC describe taxizones_view

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table: Enriched Recording Data

# COMMAND ----------

(spark.readStream
 .table("yellowtrip_bronze")
 .createOrReplaceTempView("yellowtrip_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW yellowtrip_taxizones_view AS (
# MAGIC  SELECT y.VendorID,y.DOLocationID,cast(y.passenger_count as
# MAGIC int),y.trip_distance,cast(y.total_amount as double),t.zone,t.service_zone
# MAGIC  FROM yellowtrip_bronze_tmp y
# MAGIC  INNER JOIN taxizones_view t
# MAGIC  ON y.DOLocationID = t.LocationID
# MAGIC  WHERE tpep_pickup_datetime!= tpep_dropoff_datetime and
# MAGIC  cast(y.passenger_count as int)!=0 and
# MAGIC  trip_distance!=0.0 and
# MAGIC  cast(y.total_amount as double)!=0.0)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe yellowtrip_taxizones_view

# COMMAND ----------

(spark.table("yellowtrip_taxizones_view")
 .writeStream
 .format("delta")
 .option("checkpointLocation",
f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/yellowtrip_enriched_silver")
 .outputMode("append")
 .table("yellowtrip_enriched_silver"))


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yellowtrip_enriched_silver

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM yellowtrip_enriched_silver

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table: Total Count of Passengers for each Zone, each vendorid

# COMMAND ----------

(spark.readStream
 .table("yellowtrip_enriched_silver")
 .createOrReplaceTempView("yellowtrip_enriched_silver_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW zone_passenger_count_view AS (
# MAGIC  SELECT Zone, VendorID, sum(passenger_count) as total_passenger_count
# MAGIC  FROM yellowtrip_enriched_silver_temp
# MAGIC  GROUP BY Zone, VendorID)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe zone_passenger_count_view

# COMMAND ----------

(spark.table("zone_passenger_count_view")
 .writeStream
 .format("delta")
 .outputMode("complete")
 .option("checkpointLocation",
f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/zone_passenger_count_gold")
 .table("zone_passenger_count_gold"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zone_passenger_count_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from zone_passenger_count_gold

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zone_passenger_count_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from zone_passenger_count_gold
