from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from utils import spark_session


def create_dim_paymenttype_table(spark: SparkSession):
    # Prepare the data
    data = [
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip"),
    ]

    # Define the schema
    schema = StructType(
        [
            StructField("payment_type", IntegerType(), True),
            StructField("description", StringType(), True),
        ]
    )

    # Create the DataFrame
    dim_paymenttype_df = spark.createDataFrame(data, schema)

    # Define the location in the Lakehouse where the data will be saved
    gold_table_path = "s3a://lakehouse/gold/dim_paymenttype/"

    # Save the DataFrame as a Delta table in the Gold layer
    dim_paymenttype_df.write.mode("overwrite").format("delta").save(gold_table_path)

    # Register the Gold Delta table in Hive Metastore
    # spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS default.dim_paymenttype
        USING DELTA
        LOCATION '{gold_table_path}'
        """
    )

    print(
        f"Dimension table 'dim_paymenttype' saved successfully in the Gold layer and registered in Hive as 'gold.dim_paymenttype'"
    )


if __name__ == "__main__":
    # Start Spark session using the helper function
    spark = spark_session()

    # Create and register the dim_paymenttype table
    create_dim_paymenttype_table(spark)
