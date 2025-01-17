# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

# 1. Total trip time in hours for each taxi type
total_trip_time_df = spark.sql(
    """
    SELECT taxiType, SUM((unix_timestamp(DropTime) - unix_timestamp(PickupTime)) / 3600) AS TotalTripTimeHours
    FROM YellowGreenTripCombineDelta
    GROUP BY taxiType
    """
)

# COMMAND ----------

# 2. Passenger count for each zone
passenger_count_zone_df = spark.sql(
    """
    SELECT z.Zone, SUM(y.PassengerCount) AS TotalPassengerCount
    FROM YellowGreenTripCombineDelta y
    JOIN TaxiZonesDelta z
    ON y.PickupLocationId = z.LocationID
    GROUP BY z.Zone
    """
)
passenger_count_zone_df.write.format("delta").mode("overwrite").saveAsTable("ZonePassCountTable")

# COMMAND ----------

# 3. Total trip count per zone, taxi type, trip month, and VendorID
total_trip_count_df = spark.sql(
    """
    SELECT z.Zone, y.taxiType, month(y.PickupTime) AS TripMonth, y.VendorID, COUNT(*) AS TotalTripCount
    FROM YellowGreenTripCombineDelta y
    JOIN TaxiZonesDelta z
    ON y.PickupLocationId = z.LocationID
    GROUP BY z.Zone, y.taxiType, month(y.PickupTime), y.VendorID
    """
)
total_trip_count_df.write.format("delta").mode("overwrite").saveAsTable("ZoneTaxiMonVendorTripCountTable")

# COMMAND ----------

# 4. Total trip time per borough, taxi type, trip month, and VendorID
total_trip_time_borough_df = spark.sql(
    """
    SELECT z.Borough, y.taxiType, month(y.PickupTime) AS TripMonth, y.VendorID, 
           SUM((unix_timestamp(y.DropTime) - unix_timestamp(y.PickupTime)) / 3600) AS TotalTripTimeHours
    FROM YellowGreenTripCombineDelta y
    JOIN TaxiZonesDelta z
    ON y.PickupLocationId = z.LocationID
    GROUP BY z.Borough, y.taxiType, month(y.PickupTime), y.VendorID
    """
)
total_trip_time_borough_df.write.format("delta").mode("overwrite").saveAsTable("BoroughTaxiMonVendorTripCountTable")

# COMMAND ----------

# 5. Total travel fare per service zone, taxi type, and trip month
total_fare_service_zone_df = spark.sql(
    """
    SELECT z.service_zone, y.taxiType, month(y.PickupTime) AS TripMonth, SUM(y.total_amount) AS TotalFare
    FROM YellowGreenTripCombineDelta y
    JOIN TaxiZonesDelta z
    ON y.PickupLocationId = z.LocationID
    GROUP BY z.service_zone, y.taxiType, month(y.PickupTime)
    """
)
total_fare_service_zone_df.write.format("delta").mode("overwrite").saveAsTable("TotalFareSZoneTaxiMonth")

# COMMAND ----------

# 6. Total different payment method count for each taxi type, trip month, and VendorID
total_payment_method_df = spark.sql(
    """
    SELECT y.taxiType, month(y.PickupTime) AS TripMonth, y.VendorID, p.PaymentType, COUNT(*) AS PaymentMethodCount
    FROM YellowGreenTripCombineDelta y
    JOIN PaymentTypesDelta p
    ON y.payment_type = p.PaymentTypeID
    GROUP BY y.taxiType, month(y.PickupTime), y.VendorID, p.PaymentType
    """
)
total_payment_method_df.write.format("delta").mode("overwrite").saveAsTable("PaymentCountTaxiMonthVendor")

# COMMAND ----------

# 7. Total different payment method count for each zone, taxi type, trip month, and VendorID
total_payment_method_zone_df = spark.sql(
    """
    SELECT z.Zone, y.taxiType, month(y.PickupTime) AS TripMonth, y.VendorID, p.PaymentType, COUNT(*) AS PaymentMethodCount
    FROM YellowGreenTripCombineDelta y
    JOIN TaxiZonesDelta z
    ON y.PickupLocationId = z.LocationID
    JOIN PaymentTypesDelta p
    ON y.payment_type = p.PaymentTypeID
    GROUP BY z.Zone, y.taxiType, month(y.PickupTime), y.VendorID, p.PaymentType
    """
)
total_payment_method_zone_df.write.format("delta").mode("overwrite").saveAsTable("PaymentCountZoneTaxiMonthVendor")

# COMMAND ----------

# 8. Total trip count for each "Standard Rate" applied
total_standard_rate_df = spark.sql(
    """
    SELECT y.taxiType, COUNT(*) AS StandardRateTripCount
    FROM YellowGreenTripCombineDelta y
    JOIN ratecodesdelta r
    ON y.RatecodeID = r.RateCodeID
    WHERE r.RateCode = 'Standard Rate'
    GROUP BY y.taxiType
    """
)
total_standard_rate_df.write.format("delta").mode("overwrite").saveAsTable("StandardRateTripCount")

# COMMAND ----------

# Display records from each table
print("Total Trip Time for Each Taxi Type")
total_trip_time_df.show()
print("Passenger Count for Each Zone")
passenger_count_zone_df.show()
print("Total Trip Count Per Zone, TaxiType, TripMonth, VendorID")
total_trip_count_df.show()
print("Total Trip Time Per Borough, TaxiType, TripMonth, VendorID")
total_trip_time_borough_df.show()
print("Total Travel Fare Per Service Zone, TaxiType, TripMonth")
total_fare_service_zone_df.show()
print("Total Payment Method Count Per Taxi Type, TripMonth, VendorID")
total_payment_method_df.show()
print("Total Payment Method Count Per Zone, TaxiType, TripMonth, VendorID")
total_payment_method_zone_df.show()
print("Total Standard Rate Trip Count")
total_standard_rate_df.show()

