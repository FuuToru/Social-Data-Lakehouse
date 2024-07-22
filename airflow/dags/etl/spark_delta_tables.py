import logging
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(
    appname, hive_metastore, minio_url, minio_access_key, minio_secret_key
):
    sc = (
        SparkSession.builder.appName(appname)
        .config("spark.network.timeout", "10000s")
        .config("hive.metastore.uris", hive_metastore)
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.endpoint", minio_url)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.history.fs.logDirectory", "s3a://spark-logs/")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    return sc


def extract(sc, bucket_name, raw_data_path):
    return (
        sc.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .option("mode", "DROPMALFORMED")
        .load("s3a://" + os.path.join(bucket_name, raw_data_path))
    )


# Initialize Spark session
spark = get_spark_session(
    "ETL", "thrift://hive:9083", "http://minio:9000", "root", "root12345"
)

# Set log4j
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("ETL_LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

# Creating a Delta table and registering it in Hive
delta_table_path = "s3a://raw-data/delta/bitcoin_data"
sdf = extract(spark, "raw-data", "bitcoinity_data.csv")
sdf = sdf.withColumn("Time", sdf["Time"].cast("timestamp").alias("Time"))

# Add partition column
sdf = sdf.withColumn(
    "party_ts",
    F.concat_ws(
        "-", F.year(F.col("Time")), F.month(F.col("Time")), F.dayofmonth(F.col("Time"))
    ),
)

# Save as Delta table
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy(
    "party_ts"
).save(delta_table_path)

# Create a Hive table to point to the Delta table
spark.sql(
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS delta_bitcoin (
        Time timestamp,
        bitbay double,
        bitfinex double,
        bitstamp double,
        btcmarkets double,
        cexio double,
        coinbase double,
        gemini double,
        korbit double,
        kraken double,
        others double,
        party_ts string
    )
    STORED AS DELTA
    LOCATION '{delta_table_path}'
"""
)

# Query the Delta table through Hive
sdf = spark.sql("SELECT * FROM delta_bitcoin")
sdf.printSchema()
sdf.show()

spark.stop()
