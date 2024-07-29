import logging
import os
import sys

from common import spark_session
from delta.tables import DeltaTable
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

# Schema definitions
schemas = {
    "dim_customer": """
    customer_unique_id string,
    customer_city string,
    customer_state string,
    customer_lat double,
    customer_lng double
    """,
    "dim_seller": """
    seller_id string,
    seller_zip_code_prefix integer
    """,
    "dim_review": """
    review_id string,
    review_score string
    """,
    "dim_product": """
    product_id string,
    product_category_name string,
    product_category_name_english string,
    product_name_length int,
    product_description_length int,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int
    """,
    "dim_order": """
    order_id string,
    order_status string,
    payment_type string
    """,
    "dim_date": """
    full_date date,
    year int,
    month int,
    day int,
    day_of_week int,
    week_of_year int,
    quarter int,
    is_weekend int
    """,
    "fact_table": """
    order_id string,
    order_item_id integer,
    customer_id string,
    product_id string,
    review_id string,
    seller_id string,
    full_date date,
    price double,
    freight_value double,
    payment_value double,
    payment_installments int,
    payment_sequential int
    """,
    "data_mart": """
    full_date date,
    seller_id string,
    customer_id string,
    product_id string,
    order_id string,
    order_item_id int,
    review_id string,
    price double,
    freight_value double,
    payment_value double,
    payment_installments int,
    payment_sequential int,
    order_status string,
    payment_type string,
    product_category_name string,
    product_category_name_english string,
    product_name_length int,
    product_description_length int,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int,
    customer_unique_id string,
    customer_city string,
    customer_state string,
    customer_lat double,
    customer_lng double,
    seller_zip_code_prefix int,
    year int,
    month int,
    day int,
    day_of_week int,
    week_of_year int,
    quarter int,
    is_weekend int
    """,
}

for table in tables:
    delta_table_path = f"{base_path}{table}/"
    symlink_manifest_path = f"{delta_table_path}_symlink_format_manifest/"

    # Generate Symlink Manifest
    try:
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.generate("symlink_format_manifest")
        logger.info(
            f"Generated symlink format manifest at {symlink_manifest_path} for table {table}"
        )
    except Exception as e:
        logger.error(f"Error generating symlink format manifest for table {table}: {e}")
        sys.exit(1)

    # Create External Hive Table
    sql_create_table = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {table}
    ({schemas[table]})
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{symlink_manifest_path}'
    """

    try:
        spark.sql(sql_create_table)
        logger.info(f"External Hive table {table} created successfully")
    except Exception as e:
        logger.error(f"Error creating external Hive table {table}: {e}")
        sys.exit(1)

    # Set Table Properties
    try:
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES('delta.compatibility.symlinkFormatManifest.enabled'='true')"
        )
        logger.info(f"Set TBLPROPERTIES for {table}")
    except Exception as e:
        logger.error(f"Error setting table properties for {table}: {e}")
        sys.exit(1)

logger.info("All tables created and properties set successfully")
