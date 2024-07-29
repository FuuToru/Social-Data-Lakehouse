import logging
import os
import sys

from common import spark_session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Initialize Spark session
spark = spark_session(SparkSession)

# Set Spark log level to ERROR to minimize log output
spark.sparkContext.setLogLevel("ERROR")

# Configure log4j logging
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

silver_path = "s3a://lakehouse/silver/"
bronze_path = "s3a://lakehouse/bronze/"

# Upgrade Delta tables
tables_to_upgrade = [
    "customers",
    "sellers",
    "products",
    "order_items",
    "payments",
    "order_reviews",
    "product_category",
    "orders",
    "geolocation",
]

for table in tables_to_upgrade:
    try:
        spark.sql(
            f"""
        ALTER TABLE delta.`{bronze_path}{table}`
        SET TBLPROPERTIES (
           'delta.columnMapping.mode' = 'name',
           'delta.minReaderVersion' = '2',
           'delta.minWriterVersion' = '5'
        )
        """
        )
        logger.info(f"Upgraded Delta table {table}")
    except Exception as e:
        logger.error(f"Error upgrading Delta table {table}: {e}")
        sys.exit(1)


def process_data(bronze_path, output_silver_path, process_fn):
    try:
        df = spark.read.format("delta").load(bronze_path)
        df = process_fn(df)
        print("Data Schema:")
        df.printSchema()
        print("Sample Data:")
        df.show(5)
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            output_silver_path
        )
        logger.info(
            f"Successfully processed data from {bronze_path} to {output_silver_path}"
        )
    except Exception as e:
        logger.error(
            f"Error processing data from {bronze_path} to {output_silver_path}: {e}"
        )


def process_customer_data():
    process_data(
        bronze_path=bronze_path + "customers",
        output_silver_path=silver_path + "customers",
        process_fn=lambda df: df.dropDuplicates().na.drop(),
    )


def process_seller_data():
    process_data(
        bronze_path=bronze_path + "sellers",
        output_silver_path=silver_path + "sellers",
        process_fn=lambda df: df.dropDuplicates().na.drop(),
    )


def process_product_data():
    process_data(
        bronze_path=bronze_path + "products",
        output_silver_path=silver_path + "products",
        process_fn=lambda df: df.dropDuplicates()
        .na.drop()
        .select(
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_photos_qty",
            "product_weight_g",
            col("product_description_lenght").cast("integer"),
            col("product_length_cm").cast("integer"),
            col("product_height_cm").cast("integer"),
            col("product_width_cm").cast("integer"),
        ),
    )


def process_order_item_data():
    process_data(
        bronze_path=bronze_path + "order_items",
        output_silver_path=silver_path + "order_items",
        process_fn=lambda df: df.dropDuplicates()
        .na.drop()
        .withColumn("price", col("price").cast("double"))
        .withColumn("freight_value", round(col("freight_value"), 2).cast("double")),
    )


def process_payment_data():
    process_data(
        bronze_path=bronze_path + "payments",
        output_silver_path=silver_path + "payments",
        process_fn=lambda df: df.withColumn(
            "payment_value", round(col("payment_value"), 2).cast("double")
        )
        .withColumn("payment_installments", col("payment_installments").cast("integer"))
        .na.drop()
        .dropDuplicates(),
    )


def process_order_review_data():
    process_data(
        bronze_path=bronze_path + "order_reviews",
        output_silver_path=silver_path + "order_reviews",
        process_fn=lambda df: df.drop("review_comment_title")
        .na.drop()
        .dropDuplicates(),
    )


def process_product_category_data():
    process_data(
        bronze_path=bronze_path + "product_category",
        output_silver_path=silver_path + "product_category",
        process_fn=lambda df: df.dropDuplicates().na.drop(),
    )


def process_order_data():
    process_data(
        bronze_path=bronze_path + "orders",
        output_silver_path=silver_path + "orders",
        process_fn=lambda df: df.na.drop().dropDuplicates(["order_id"]),
    )


def process_geolocation_data():
    process_data(
        bronze_path=bronze_path + "geolocation",
        output_silver_path=silver_path + "geolocation",
        process_fn=lambda df: df.dropDuplicates()
        .na.drop()
        .filter(
            (col("geolocation_lat") <= 5.27438888)
            & (col("geolocation_lng") >= -73.98283055)
            & (col("geolocation_lat") >= -33.75116944)
            & (col("geolocation_lng") <= -34.79314722)
        ),
    )


# Process each dataset and write to the Silver layer
process_customer_data()
process_seller_data()
process_product_data()
process_order_item_data()
process_payment_data()
process_order_review_data()
process_product_category_data()
process_order_data()
process_geolocation_data()
