import logging
import os
import sys

from common import spark_session
from pyspark.sql import SparkSession

# Initialize Spark session
spark = spark_session(SparkSession)

# Set Spark log level to ERROR to minimize log output
spark.sparkContext.setLogLevel("ERROR")

# Configure log4j logging
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)


def read_and_write_to_delta(file_path, output_path):
    try:
        logger.info(f"Reading data from {file_path}")
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("delimiter", ",")
            .option("mode", "DROPMALFORMED")
            .csv(file_path)
        )

        logger.info(f"Writing data to Delta")
        df.printSchema()
        df.show()

        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            output_path
        )
        logger.info(f"Data successfully written to {output_path}")
    except Exception as e:
        logger.error(f"Error while processing {file_path}: {e}")


# List of files and corresponding table names
files_and_tables = {
    "s3a://lakehouse/raw/olist_sellers_dataset.csv": "s3a://lakehouse/bronze/sellers",
    "s3a://lakehouse/raw/olist_customers_dataset.csv": "s3a://lakehouse/bronze/customers",
    "s3a://lakehouse/raw/olist_geolocation_dataset.csv": "s3a://lakehouse/bronze/geolocation",
    "s3a://lakehouse/raw/olist_order_items_dataset.csv": "s3a://lakehouse/bronze/order_items",
    "s3a://lakehouse/raw/olist_order_payments_dataset.csv": "s3a://lakehouse/bronze/payments",
    "s3a://lakehouse/raw/olist_order_reviews_dataset.csv": "s3a://lakehouse/bronze/order_reviews",
    "s3a://lakehouse/raw/olist_orders_dataset.csv": "s3a://lakehouse/bronze/orders",
    "s3a://lakehouse/raw/product_category_name_translation.csv": "s3a://lakehouse/bronze/product_category",
    "s3a://lakehouse/raw/olist_products_dataset.csv": "s3a://lakehouse/bronze/products",
}

# Load each file into the corresponding Delta table
for file_path, table_name in files_and_tables.items():
    read_and_write_to_delta(file_path, table_name)
