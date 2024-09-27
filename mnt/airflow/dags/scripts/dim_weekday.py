from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from utils import spark_session


def create_dim_weekday_table(spark: SparkSession):
    # Prepare the data
    data = [
        (1, "Sunday"),
        (2, "Monday"),
        (3, "Tuesday"),
        (4, "Wednesday"),
        (5, "Thursday"),
        (6, "Friday"),
        (7, "Saturday"),
    ]

    # Define the schema
    schema = StructType(
        [
            StructField("pickup_weekday", IntegerType(), True),
            StructField("description", StringType(), True),
        ]
    )

    # Create the DataFrame
    dim_weekday_df = spark.createDataFrame(data, schema)

    # Define the location in the Lakehouse where the data will be saved
    gold_table_path = "s3a://lakehouse/gold/dim_weekday/"

    # Save the DataFrame as a Delta table in the Gold layer
    dim_weekday_df.write.mode("overwrite").format("delta").save(gold_table_path)

    # Register the Gold Delta table in Hive Metastore
    # spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS default.dim_weekday
        USING DELTA
        LOCATION '{gold_table_path}'
        """
    )

    print(
        f"Dimension table 'dim_weekday' saved successfully in the Gold layer and registered in Hive as 'gold.dim_weekday'"
    )


if __name__ == "__main__":
    # Start Spark session using the helper function
    spark = spark_session()

    # Create and register the dim_weekday table
    create_dim_weekday_table(spark)
