# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata/yellow_tripdata_2019_01.csv

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata/yellow_tripdata_2019_06.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
 query = (spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", source_format)
 .option("cloudFiles.schemaLocation", checkpoint_directory)
 .load(data_source)
 .writeStream
 .option("checkpointLocation", checkpoint_directory)
 .option("mergeSchema", "true")
 .table(table_name))
 return query


# COMMAND ----------

query = autoload_to_table(data_source =
"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata",
 source_format = "csv",
 table_name = "yellowtripsrc",
 checkpoint_directory =
"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/yellowtripsrc")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM yellowtripsrc

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE yellowtripsrc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vendor_trip_count AS
# MAGIC  SELECT VendorID, count(*) total_trip_count
# MAGIC  FROM yellowtripsrc
# MAGIC  GROUP BY VendorID;
# MAGIC
# MAGIC SELECT * FROM vendor_trip_count

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata

# COMMAND ----------

files = dbutils.fs.ls(f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/yellow_tripdata")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vendor_trip_count

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY yellowtripsrc

# COMMAND ----------

(spark.readStream
 .table("yellowtripsrc")
 .createOrReplaceTempView("streaming_yellowtrip_vw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_yellowtrip_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VendorID, count(*) total_trip_count
# MAGIC  FROM streaming_yellowtrip_vw
# MAGIC  GROUP BY VendorID;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vendor_trip_count_vw AS
# MAGIC  SELECT VendorID, count(*) total_trip_count
# MAGIC  FROM streaming_yellowtrip_vw
# MAGIC  GROUP BY VendorID;

# COMMAND ----------

(spark.table("vendor_trip_count_vw")
 .writeStream
 .option("checkpointLocation",
f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/vendor_trip_count_table")
 .outputMode("complete")
 .trigger(availableNow=True)
 .table("vendor_trip_count_table")
 .awaitTermination()
# This optional method blocks execution of the next cell until the incremental batch write has succeeded
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vendor_trip_count_table;

# COMMAND ----------

query = (spark.table("vendor_trip_count_vw")
 .writeStream
 .option("checkpointLocation",
f"dbfs:/FileStore/tables/nyctaxidata/landing/yellowdata/checkpoint/vendor_trip_count_table")
 .outputMode("complete")
 .trigger(processingTime='4 seconds')
 .table("vendor_trip_count_table"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vendor_trip_count_table
