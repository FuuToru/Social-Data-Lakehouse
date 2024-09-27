import argparse

from pyspark.sql import SparkSession
from utils import spark_session


def append_silver_to_gold(spark: SparkSession, year: int, month: int):
    # Define paths
    silver_path = f"s3a://lakehouse/silver/{year}/{month:02d}/"
    gold_table_path = "s3a://lakehouse/gold/fact_trip/"

    # Load the Silver layer data
    silver_df = spark.read.format("delta").load(silver_path)

    # Check if the Gold Delta table exists
    if spark._jsparkSession.catalog().tableExists("default", "fact_trip"):
        # Append the Silver data to the Gold table
        silver_df.write.format("delta").option("mergeSchema", "true").mode(
            "append"
        ).save(gold_table_path)

        print(
            f"Data from silver layer for {year}-{month:02d} successfully appended into Gold layer."
        )
    else:
        # If the Gold table doesn't exist, write the Silver data as a new Delta table and register it in Hive
        silver_df.write.option("mergeSchema", "true").format("delta").mode(
            "overwrite"
        ).save(gold_table_path)

        # Register the Gold Delta table in the Hive Metastore
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS default.fact_trip
            USING DELTA
            LOCATION '{gold_table_path}'
            """
        )

        print(
            f"Gold Delta table created and registered in Hive Metastore, data for {year}-{month:02d} written to Gold layer."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Append data from silver to gold Delta table"
    )
    parser.add_argument("--year", type=int, required=True, help="Year of the data")
    parser.add_argument("--month", type=int, required=True, help="Month of the data")

    args = parser.parse_args()

    # Start Spark session using the helper function
    spark = spark_session()

    # Perform the append operation
    append_silver_to_gold(spark, args.year, args.month)
