from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from utils import spark_session


def process_and_save_location_data(spark: SparkSession):
    # Define the CSV file path in the Lakehouse (S3)
    csv_path = "s3a://lakehouse/raw/taxi_zone_lookup.csv"

    # Define the schema for the CSV file
    schema = StructType(
        [
            StructField("LocationID", IntegerType(), True),
            StructField("Borough", StringType(), True),
            StructField("Zone", StringType(), True),
            StructField("service_zone", StringType(), True),
        ]
    )

    # Load the CSV file into a DataFrame
    dim_location_df = (
        spark.read.format("csv").schema(schema).option("header", "true").load(csv_path)
    )

    # Rename columns to match naming conventions
    dim_location_df = (
        dim_location_df.withColumnRenamed("LocationID", "locationId")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
    )

    # Define the path in the Lakehouse (S3) where the data will be saved as a Delta table
    gold_table_path = "s3a://lakehouse/gold/dim_location/"

    # Save the DataFrame as a Delta table in the Gold layer
    dim_location_df.write.mode("overwrite").format("delta").save(gold_table_path)

    # Register the Gold Delta table in the Hive Metastore
    # spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS default.dim_location
        USING DELTA
        LOCATION '{gold_table_path}'
        """
    )

    print(
        f"Dimension table 'dim_location' saved successfully in the Gold layer and registered in Hive as 'gold.dim_location'"
    )


if __name__ == "__main__":
    # Start Spark session using the helper function
    spark = spark_session()

    # Process and save location data
    process_and_save_location_data(spark)
