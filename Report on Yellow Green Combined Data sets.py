# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM yellowgreentripcombinedelta

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE yellowgreentripcombinedelta

# COMMAND ----------

# 1. Total trip time in hours for each taxi type
total_trip_time_df = spark.sql(
    """
    SELECT taxiType, SUM((unix_timestamp(DropTime) - unix_timestamp(PickupTime)) / 3600) AS TotalTripTimeHours
    FROM YellowGreenTripCombineDelta
    GROUP BY taxiType
    """
)

display(total_trip_time_df)

# COMMAND ----------

# 2. Taxi-type-wise total trip time in hours for each trip-month
total_trip_time_month_df = spark.sql(
    """
    SELECT taxiType, month(PickupTime) AS TripMonth, 
           SUM((unix_timestamp(DropTime) - unix_timestamp(PickupTime)) / 3600) AS TotalTripTimeHours
    FROM YellowGreenTripCombineDelta
    GROUP BY taxiType, month(PickupTime)
    """
)

display(total_trip_time_month_df)

# COMMAND ----------

# 3. Taxi-type-wise total number of passengers for each trip month
total_passengers_month_df = spark.sql(
    """
    SELECT taxiType, month(PickupTime) AS TripMonth, SUM(PassengerCount) AS TotalPassengers
    FROM YellowGreenTripCombineDelta
    GROUP BY taxiType, month(PickupTime)
    """
)
display(total_passengers_month_df)

# COMMAND ----------

# 4. Taxi-type-wise total number of payments for each payment-type
total_payments_df = spark.sql(
    """
    SELECT taxiType, payment_type, COUNT(*) AS TotalPayments
    FROM YellowGreenTripCombineDelta
    GROUP BY taxiType, payment_type
    """
)
display(total_payments_df)

# COMMAND ----------

# 5. Total number of light trips for each taxi type (PassengerCount <= 2)
light_trips_df = spark.sql(
    """
    SELECT taxiType, COUNT(*) AS TotalLightTrips
    FROM YellowGreenTripCombineDelta
    WHERE PassengerCount <= 2
    GROUP BY taxiType
    """
)

display(light_trips_df)

# COMMAND ----------

# 6. Total number of light trips for each taxi type month-wise (PassengerCount <= 2)
light_trips_month_df = spark.sql(
    """
    SELECT taxiType, month(PickupTime) AS TripMonth, COUNT(*) AS TotalLightTrips
    FROM YellowGreenTripCombineDelta
    WHERE PassengerCount <= 2
    GROUP BY taxiType, month(PickupTime)
    """
)
# Save light trips month-wise report to a Delta table
light_trips_month_df.write.format("delta").mode("overwrite").saveAsTable("LightTripsMonthWise")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM LightTripsMonthWise

# COMMAND ----------

# 7. Total number of fully-loaded trips for each taxi type (PassengerCount >= 4)
fully_loaded_trips_df = spark.sql(
    """
    SELECT taxiType, COUNT(*) AS TotalFullyLoadedTrips
    FROM YellowGreenTripCombineDelta
    WHERE PassengerCount >= 4
    GROUP BY taxiType
    """
)

display(fully_loaded_trips_df)

# COMMAND ----------

# 8. Total number of fully-loaded trips for each taxi type month-wise (PassengerCount >= 4)
fully_loaded_trips_month_df = spark.sql(
    """
    SELECT taxiType, month(PickupTime) AS TripMonth, COUNT(*) AS TotalFullyLoadedTrips
    FROM YellowGreenTripCombineDelta
    WHERE PassengerCount >= 4
    GROUP BY taxiType, month(PickupTime)
    """
)

# Save fully-loaded trips month-wise report to a Delta table
fully_loaded_trips_month_df.write.format("delta").mode("overwrite").saveAsTable("FullyLoadedTripsMonthWise")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM FullyLoadedTripsMonthWise

# COMMAND ----------


# 9. Total number of midnight trips for each taxi type (12AM to 4AM)
midnight_trips_df = spark.sql(
    """
    SELECT taxiType, COUNT(*) AS TotalMidnightTrips
    FROM YellowGreenTripCombineDelta
    WHERE hour(PickupTime) >= 0 AND hour(PickupTime) < 4
    GROUP BY taxiType
    """
)

display(midnight_trips_df)

# COMMAND ----------

# 10. Total number of midnight trips for each taxi type month-wise (12AM to 4AM)
midnight_trips_month_df = spark.sql(
    """
    SELECT taxiType, month(PickupTime) AS TripMonth, COUNT(*) AS TotalMidnightTrips
    FROM YellowGreenTripCombineDelta
    WHERE hour(PickupTime) >= 0 AND hour(PickupTime) < 4
    GROUP BY taxiType, month(PickupTime)
    """
)

# Save midnight trips month-wise report to a Delta table
midnight_trips_month_df.write.format("delta").mode("overwrite").saveAsTable("MidnightTripsMonthWise")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM MidnightTripsMonthWise
