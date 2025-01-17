# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

yellow_tripdata_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/lkmpro/yellow/yellow_tripdata_2019_06.csv")

# COMMAND ----------

# a. Create a temporary view "YellowTripRawDataView"
yellow_tripdata_df.createOrReplaceTempView("YellowTripRawDataView")

# COMMAND ----------

# b. Filter the data with the specified conditions
filtered_yellow_tripdata_df = spark.sql(
    """
    SELECT *
    FROM YellowTripRawDataView
    WHERE 
        tpep_pickup_datetime IS NOT NULL AND 
        tpep_dropoff_datetime IS NOT NULL AND 
        tpep_pickup_datetime != tpep_dropoff_datetime AND 
        year(tpep_pickup_datetime) = 2019 AND 
        passenger_count > 0 AND 
        trip_distance > 0 AND 
        fare_amount > 0 AND 
        total_amount > 0 AND 
        PULocationID IS NOT NULL AND 
        DOLocationID IS NOT NULL
    """
)

# COMMAND ----------

# c. Create a Delta Table "YellowTripDelta" with the specified columns
filtered_yellow_tripdata_df.select(
    col("VendorID"),
    col("tpep_pickup_datetime").alias("PickupTime"),
    col("tpep_dropoff_datetime").alias("DropTime"),
    col("passenger_count").alias("PassengerCount"),
    col("trip_distance").alias("TripDistance"),
    col("RatecodeID"),
    col("PULocationID").alias("PickupLocationId"),
    col("DOLocationID").alias("DropLocationId"),
    col("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("congestion_surcharge")
).write.format("delta").mode("overwrite").saveAsTable("YellowTripDelta")

print("Delta table 'YellowTripDelta' created successfully!")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE YellowTripDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM YellowTripDelta;

# COMMAND ----------

green_tripdata_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/lkmpro/green/green_tripdata_2019-06.csv")

# COMMAND ----------

# a. Create a temporary view "GreenTripRawDataView"
green_tripdata_df.createOrReplaceTempView("GreenTripRawDataView")

# COMMAND ----------

# b. Filter the data with the specified conditions
filtered_green_tripdata_df = spark.sql(
    """
    SELECT *
    FROM GreenTripRawDataView
    WHERE 
        lpep_pickup_datetime IS NOT NULL AND 
        lpep_dropoff_datetime IS NOT NULL AND 
        lpep_pickup_datetime != lpep_dropoff_datetime AND 
        year(lpep_pickup_datetime) = 2019 AND 
        passenger_count > 0 AND 
        trip_distance > 0 AND 
        fare_amount > 0 AND 
        total_amount > 0 AND 
        PULocationID IS NOT NULL AND 
        DOLocationID IS NOT NULL
    """
)

# COMMAND ----------

# c. Create a Delta Table "GreenTripDelta" with the specified columns
filtered_green_tripdata_df.select(
    col("VendorID"),
    col("lpep_pickup_datetime").alias("PickupTime"),
    col("lpep_dropoff_datetime").alias("DropTime"),
    col("passenger_count").alias("PassengerCount"),
    col("trip_distance").alias("TripDistance"),
    col("RatecodeID"),
    col("PULocationID").alias("PickupLocationId"),
    col("DOLocationID").alias("DropLocationId"),
    col("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("congestion_surcharge")
).write.format("delta").mode("overwrite").saveAsTable("GreenTripDelta")

print("Delta table 'GreenTripDelta' created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE GreenTripDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM GreenTripDelta;

# COMMAND ----------

# Load YellowTripDelta and GreenTripDelta tables
yellow_trip_df = spark.table("YellowTripDelta").withColumn("taxiType", lit("Yellow"))
green_trip_df = spark.table("GreenTripDelta").withColumn("taxiType", lit("Green"))

# Combine the Yellow and Green trip data
combined_trip_df = yellow_trip_df.unionByName(green_trip_df)

# Save the combined data to a Delta table "YellowGreenTripCombineDelta"
combined_trip_df.write.format("delta").mode("overwrite").saveAsTable("YellowGreenTripCombineDelta")

print("Delta table 'YellowGreenTripCombineDelta' created successfully!")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE YellowGreenTripCombineDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM YellowGreenTripCombineDelta;

# COMMAND ----------

taxi_zone_data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/lkmpro/common/TaxiZones.csv")

# COMMAND ----------

# a. Create a temporary view ""
taxi_zone_data_df.createOrReplaceTempView("TaxiZonesRawDataView")

# COMMAND ----------

# b. Create a Delta Table ""
spark.sql("""
    CREATE OR REPLACE TABLE TaxiZonesDelta
    USING DELTA
    AS SELECT * FROM TaxiZonesRawDataView
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE TaxiZonesDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM TaxiZonesDelta

# COMMAND ----------

payment_types_data_df = spark.read.format("json").option("inferSchema", "true").load("dbfs:/mnt/lkmpro/common/PaymentTypes")

# COMMAND ----------

# a. Create a temporary view ""
payment_types_data_df.createOrReplaceTempView("PaymentTypesRawDataView")

# COMMAND ----------

# b. Create a Delta Table 

spark.sql(""" 
    CREATE OR REPLACE TABLE PaymentTypesDelta
    USING DELTA
    AS SELECT * FROM PaymentTypesRawDataView
          """)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE PaymentTypesDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PaymentTypesDelta

# COMMAND ----------

rate_codes_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/lkmpro/common/RateCodes.csv")

# COMMAND ----------

# a. Create a temporary view ""
rate_codes_df.createOrReplaceTempView("RateCodesRawDataView")

# COMMAND ----------

# b. Create a Delta Table ""
spark.sql("""
    CREATE OR REPLACE TABLE RateCodesDelta
    USING DELTA
    AS SELECT * FROM RateCodesRawDataView
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE RateCodesDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM RateCodesDelta
