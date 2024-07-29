import logging
import sys

from common import spark_session
from pyspark.sql import SparkSession

# Initialize Spark session
spark = spark_session(SparkSession)

# Set log4j logging
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

# Base paths
base_path = "s3a://lakehouse/gold/"
tables = [
    "dim_customer",
    "dim_seller",
    "dim_review",
    "dim_product",
    "dim_order",
    "dim_date",
    "fact_table",
    "data_mart",
]

for table in tables:
    delta_table_path = f"{base_path}{table}/"

    try:
        logger.info(f"Loading table {table} from {delta_table_path}")
        df = spark.read.format("delta").load(delta_table_path)
        logger.info(f"Successfully loaded table {table}")

        print(f"Schema for table {table}:")
        df.printSchema()
        print(f"Sample data for table {table}:")
        df.show(5)
    except Exception as e:
        logger.error(f"Error loading or printing schema for table {table}: {e}")
        sys.exit(1)
