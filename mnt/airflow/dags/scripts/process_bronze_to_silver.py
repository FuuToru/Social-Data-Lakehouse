import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2, unix_timestamp
from utils import spark_session

# Updated Columns to drop (keeping tip_amount and fare_amount)
columns_to_drop = [
    "store_and_fwd_flag",
    "RateCodeID",
    "extra",
    "mta_tax",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "Trip_type",
    "congestion_surcharge",
]


def process_bronze_to_silver(spark: SparkSession, year: int, month: int):
    # Define paths
    raw_path = f"s3a://lakehouse/raw/{year}/{month:02d}/green_tripdata_{year}-{month:02d}.parquet"
    silver_path = f"s3a://lakehouse/silver/{year}/{month:02d}/"

    # Load the raw data
    df = spark.read.parquet(raw_path)

    # Drop unnecessary columns and rows with missing values
    df = df.drop(*columns_to_drop).dropna()

    # Select necessary columns, convert data types, and rename columns
    df = df.select(
        col("VendorID").alias("vendor_id"),  # Retain VendorID for trip_id generation
        col("lpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("lpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("PULocationID").cast("int").alias("pickup_locationId"),
        col("DOLocationID").cast("int").alias("dropoff_locationId"),
        col("payment_type").cast("int").alias("payment_type"),
        col("passenger_count").cast("int").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("total_amount").cast("double").alias("total_amount"),
        col("fare_amount").cast("double").alias("fare_amount"),  # Keep fare_amount
        col("tip_amount").cast("double").alias("tip_amount"),  # Keep tip_amount
    )

    # Add new columns based on pickup_datetime (year, month, day, hour, minute, weekday)
    df = (
        df.withColumn("pickup_year", F.year(col("pickup_datetime")))
        .withColumn("pickup_month", F.month(col("pickup_datetime")))
        .withColumn("pickup_day", F.dayofmonth(col("pickup_datetime")))
        .withColumn("pickup_hour", F.hour(col("pickup_datetime")))
        .withColumn("pickup_minute", F.minute(col("pickup_datetime")))
        .withColumn("pickup_weekday", F.dayofweek(col("pickup_datetime")))
    )

    # Generate surrogate key (trip_id) by hashing VendorID, lpep_pickup_datetime, and PULocationID
    df = df.withColumn(
        "trip_id",
        sha2(
            concat_ws(
                "_", col("vendor_id"), col("pickup_datetime"), col("pickup_locationId")
            ),
            256,
        ),
    )

    # Calculate trip_duration (in minutes)
    df = df.withColumn(
        "trip_duration",
        (
            unix_timestamp(col("dropoff_datetime"))
            - unix_timestamp(col("pickup_datetime"))
        )
        / 60,
    )

    # Drop the dropoff_datetime column as it's no longer needed
    df = df.drop("dropoff_datetime")

    # Filter data to keep only values within the appropriate ranges and add conditions for tip_amount and fare_amount
    df = df.filter(
        (col("pickup_locationId").between(1, 265))
        & (col("dropoff_locationId").between(1, 265))
        & (col("payment_type").between(1, 5))
        & (col("passenger_count").between(1, 6))
        & (col("trip_distance") > 0)
        & (col("total_amount") > 0)
        & (col("fare_amount") > 0)  # Ensure fare_amount > 0
        & (col("tip_amount") >= 0)  # Ensure tip_amount >= 0
        & (col("pickup_year").between(2023, 2024))
        & (col("pickup_month").between(1, 12))
        & (col("pickup_day").between(1, 31))
        & (col("pickup_hour").between(0, 23))
        & (col("pickup_minute").between(0, 59))
        & (col("trip_duration") > 0)
    )

    # Save the cleaned data to the silver layer
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(
        silver_path
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process bronze data to silver")
    parser.add_argument("--year", type=int, required=True, help="Year of the data")
    parser.add_argument("--month", type=int, required=True, help="Month of the data")

    args = parser.parse_args()
    spark = spark_session()
    process_bronze_to_silver(spark, args.year, args.month)
