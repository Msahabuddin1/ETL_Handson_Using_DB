# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata/yellow_tripdata_2019_01.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellow_green_commondata/taxizones/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellow_green_commondata/taxizones/TaxiZones.csv

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/tables/nyctaxidata/dltphase1/dlt_demo_10_jan_2023/storage")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/tables/nyctaxidata/dltphase1/dlt_demo_10_jan_2023/storage/system/events")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC delta.`dbfs:/FileStore/tables/nyctaxidata/dltphase1/dlt_demo_10_jan_2023/storage/system/events`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/tables/nyctaxidata/dltphase1/dlt_demo_10_jan_2023/storage/tables/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM raju_chal_dlt_demo_10_jan_2023.vendor_zone_passenger_count

# COMMAND ----------

# MAGIC %md
# MAGIC Upload another yellow trip data file into DBFS location
# MAGIC dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC Feel free to run it a couple more times if desired.
# MAGIC Following this, run the pipeline again and view the results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM raju_chal_dlt_demo_10_jan_2023.vendor_zone_passenger_count

# COMMAND ----------

# MAGIC %md
# MAGIC Upload another yellow trip data file into DBFS location
# MAGIC dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM raju_chal_dlt_demo_10_jan_2023.vendor_zone_passenger_count

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use raju_chal_dlt_demo_10_jan_2023

# COMMAND ----------

# MAGIC %sql
# MAGIC use raju_chal_dlt_demo_10_jan_2023

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raju_chal_dlt_demo_10_jan_2023.yellowtrip_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raju_chal_dlt_demo_10_jan_2023.yellotrip_taxizones_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raju_chal_dlt_demo_10_jan_2023.yellowtrip_bronze limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC
