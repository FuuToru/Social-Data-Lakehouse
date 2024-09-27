import argparse
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from utils import spark_session

# Define the expected schema and columns
expected_columns = [
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "pickup_locationId",
    "dropoff_locationId",
    "payment_type",
    "passenger_count",
    "trip_distance",
    "total_amount",
    "fare_amount",  # Added fare_amount
    "tip_amount",  # Added tip_amount
    "pickup_year",
    "pickup_month",
    "pickup_day",
    "pickup_hour",
    "pickup_minute",
    "pickup_weekday",
    "trip_duration",
]

expected_schema = {
    "trip_id": StringType(),
    "vendor_id": IntegerType(),
    "pickup_datetime": TimestampType(),
    "pickup_locationId": IntegerType(),
    "dropoff_locationId": IntegerType(),
    "payment_type": IntegerType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "total_amount": DoubleType(),
    "fare_amount": DoubleType(),  # Added fare_amount to schema
    "tip_amount": DoubleType(),  # Added tip_amount to schema
    "pickup_year": IntegerType(),
    "pickup_month": IntegerType(),
    "pickup_day": IntegerType(),
    "pickup_hour": IntegerType(),
    "pickup_minute": IntegerType(),
    "pickup_weekday": IntegerType(),
    "trip_duration": DoubleType(),
}


def validate_silver_table(spark: SparkSession, year: int, month: int):
    # Path to the silver layer
    silver_path = f"s3a://lakehouse/silver/{year}/{month:02d}/"

    # Load the silver data
    df = spark.read.format("delta").load(silver_path)

    # Check if all expected columns exist
    df_columns = set(df.columns)
    missing_columns = [col for col in expected_columns if col not in df_columns]

    if missing_columns:
        raise ValueError(f"Missing columns in the dataset: {missing_columns}")
    else:
        print("All expected columns are present.")

    # Validate column types
    for col_name, expected_type in expected_schema.items():
        actual_type = df.schema[col_name].dataType
        if not isinstance(actual_type, type(expected_type)):
            raise TypeError(
                f"Column {col_name} has wrong type: {actual_type}. Expected {expected_type}."
            )

    print("All columns have correct types.")

    # Filter conditions for validation, including fare_amount > 0 and tip_amount >= 0
    filter_conditions = [
        df["pickup_locationId"].between(1, 265),
        df["dropoff_locationId"].between(1, 265),
        df["payment_type"].between(1, 5),
        df["passenger_count"].between(1, 6),
        df["trip_distance"] > 0,
        df["total_amount"] > 0,
        df["fare_amount"] > 0,  # Validate fare_amount > 0
        df["tip_amount"] >= 0,  # Validate tip_amount >= 0
        df["pickup_year"].between(2023, 2024),
        df["pickup_month"].between(1, 12),
        df["pickup_day"].between(1, 31),
        df["pickup_hour"].between(0, 23),
        df["pickup_minute"].between(0, 59),
        df["trip_duration"] > 0,
    ]

    # Combine conditions using reduce and logical AND
    combined_conditions = reduce(lambda a, b: a & b, filter_conditions)

    # Apply the combined filter
    valid_df = df.filter(combined_conditions)
    invalid_count = df.count() - valid_df.count()

    if invalid_count > 0:
        print(f"Validation failed: {invalid_count} invalid rows found.")
    else:
        print("All rows are valid.")

    print("Validation complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate silver table data")
    parser.add_argument("--year", type=int, required=True, help="Year of the data")
    parser.add_argument("--month", type=int, required=True, help="Month of the data")

    args = parser.parse_args()
    spark = spark_session()
    validate_silver_table(spark, args.year, args.month)
