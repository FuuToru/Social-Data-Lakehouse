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


# Function to drop all tables in the Hive catalog
def drop_all_hive_tables(schema="default"):
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
        for table in tables:
            table_name = table["tableName"]
            spark.sql(f"DROP TABLE {schema}.{table_name}")
            logger.info(f"Dropped table {schema}.{table_name}")
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        sys.exit(1)


# Drop all tables in the default schema
drop_all_hive_tables()

logger.info("All tables dropped successfully")

spark.stop()
