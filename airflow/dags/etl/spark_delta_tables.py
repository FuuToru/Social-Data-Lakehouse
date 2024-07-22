import logging
import os
import sys

# from delta import DeltaTable
from delta.tables import DeltaTable
from pyspark import StorageLevel
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


# spark session
spark = get_spark_session(
    "ETL", "thrift://hive:9083", "http://minio:9000", "root", "root12345"
)

# Set log4j
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("ETL_LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

sql_delta_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS bitcoinss (
    time timestamp, 
    bitbay double, 
    bitfinex double, 
    bitstamp double, 
    party_ts string
)
PARTITIONED BY (party_ts)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3a://raw-data/delta/bitcoin_data/_symlink_format_manifest/'
"""

spark.sql(sql_delta_table)
spark.sql("MSCK REPAIR TABLE bitcoinss")
spark.sql(
    "ALTER TABLE bitcoinss SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)"
)

sdf = spark.sql("SELECT * FROM bitcoinss")
sdf.printSchema()

spark.stop()
