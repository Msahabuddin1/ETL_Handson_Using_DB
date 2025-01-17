# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/lkmpro/yellow

# COMMAND ----------

""" Create RDD to store valid yellow trip records for June Month of Year 2019
(Filter conditions to be checked)"""

from pyspark.sql.functions import *

# Read data from DBFS landing location
yellow_tripdata_path = "dbfs:/mnt/lkmpro/yellow/yellow_tripdata_2019_06.csv"
yellow_tripdata_df = spark.read.csv(yellow_tripdata_path, header=True, inferSchema=True)

# Filter valid records for June 2019
filtered_rdd = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).rdd

# Display sample filtered records
filtered_rdd.take(10)


# COMMAND ----------

# Total valid trip count for each vendor for June
total_trip_count = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).groupBy("VendorID").count()

# Display the result
total_trip_count.show()

# COMMAND ----------

# Total trip cost for each vendor for June
total_trip_cost = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).groupBy("VendorID").sum("total_amount")

# Rename column for better understanding
total_trip_cost = total_trip_cost.withColumnRenamed("sum(total_amount)", "total_trip_cost")

# Display the result
total_trip_cost.show()


# COMMAND ----------

# Total passenger count for each vendor for June
total_passenger_count = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).groupBy("VendorID").sum("passenger_count")

# Rename column for better understanding
total_passenger_count = total_passenger_count.withColumnRenamed("sum(passenger_count)", "total_passenger_count")

# Display the result
total_passenger_count.show()


# COMMAND ----------

# Total trip distance for each vendor for June
total_trip_distance = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).groupBy("VendorID").sum("trip_distance")

# Rename column for better understanding
total_trip_distance = total_trip_distance.withColumnRenamed("sum(trip_distance)", "total_trip_distance")

# Display the result
total_trip_distance.show()


# COMMAND ----------

# Total passenger count for each day for June
daily_passenger_count = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("pickup_date")).sum("passenger_count")

# Rename column for better understanding
daily_passenger_count = daily_passenger_count.withColumnRenamed("sum(passenger_count)", "total_passenger_count")

# Display the result
daily_passenger_count.show()


# COMMAND ----------

# Generate total passenger count for each Vendor and each day of June
vendor_day_passenger_count = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") != col("tpep_dropoff_datetime")) &
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (year(col("tpep_pickup_datetime")) == 2019) &
    (month(col("tpep_pickup_datetime")) == 6)
).groupBy(
    "VendorID",
    date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("pickup_date")
).sum("passenger_count")

# Rename column for better understanding
vendor_day_passenger_count = vendor_day_passenger_count.withColumnRenamed("sum(passenger_count)", "total_passenger_count")

# Display the result
vendor_day_passenger_count.show()

# COMMAND ----------

# Save results to DBFS location
output_path = "dbfs:/FileStore/tables/nyctaxidata/stage3_spark/passcount_vendorday"
vendor_day_passenger_count.write.csv(output_path, header=True, mode="overwrite")

print(f"Results saved to {output_path}")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/nyctaxidata/stage3_spark/passcount_vendorday")
