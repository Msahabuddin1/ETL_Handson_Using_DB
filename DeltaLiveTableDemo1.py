# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellowdata/yellow_tripdata/yellow_tripdata_2019_01.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellow_green_commondata/taxizones/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/nyctaxidata/dltlanding/yellow_green_commondata/taxizones/TaxiZones.csv
