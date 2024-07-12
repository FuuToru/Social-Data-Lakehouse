import logging
import os
import sys

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(appname, minio_url, minio_access_key, minio_secret_key):
    sc = (
        SparkSession.builder.appName(appname)
        .config("spark.network.timeout", "10000s")
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


# Spark session
spark = get_spark_session("ETL", "http://minio:9000", "minio", "minio12345")

# Set log4j
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("ETL_LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

# Extract data
sdf = extract(spark, "raw-data", "bitcoinity_data.csv")
sdf = sdf.withColumn("Time", sdf["Time"].cast("timestamp").alias("Time"))

# Add new column 'party_ts' with formatted timestamp
sdf = sdf.withColumn(
    "party_ts",
    F.concat_ws(
        "-",
        F.year(F.col("Time")),
        F.month(F.col("Time")),
        F.dayofmonth(F.col("Time")),
    ),
)

# Show schema
sdf.printSchema()

# Perform upsert using Delta Lake
delta_table = DeltaTable.forPath(spark, "s3a://raw-data/delta/party")

delta_table.alias("t1").merge(
    sdf.alias("t2"), "t1.Time = t2.Time"
).whenNotMatchedInsertAll().execute()

# Read and show the merged data
spark.read.format("delta").load("s3a://raw-data/delta/party").repartition(1).show()

spark.stop()
