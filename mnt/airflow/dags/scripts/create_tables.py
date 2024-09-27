from pyspark.sql import SparkSession

# Initialize the Spark session with Hive support
spark = (
    SparkSession.builder.appName("Create Gold Schema and Tables")
    .enableHiveSupport()
    .getOrCreate()
)

# Create the gold schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Now create the fact_trip table (using Delta Lake format)
gold_table_path = "s3a://lakehouse/gold/fact_trip/"

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS gold.fact_trip
    USING DELTA
    LOCATION '{gold_table_path}'
"""
)

# Stop the Spark session
spark.stop()
