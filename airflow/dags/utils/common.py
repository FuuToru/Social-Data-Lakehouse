import logging
import os
import sys

from pyspark.sql import functions as F

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
AWS_S3_ENDPOINT = os.environ["AWS_S3_ENDPOINT"]
AWS_BUCKET_NAME = "lakehouse"


def spark_session(spark_session):
    spark = (
        spark_session.builder.appName("Ingest checkin table into bronze")
        .master("spark://spark-master:7077")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", f"s3a://{AWS_BUCKET_NAME}/")
        .config(
            "spark.driver.extraClassPath",
            "/opt/bitnami/spark/jars/hadoop-aws-3.3.6.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar:/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar:/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar:/opt/bitnami/spark/jars/delta-storage-2.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.367.jar:/opt/bitnami/spark/jars/hadoop-common-3.3.6.jar",
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

    return spark


def date_partition(ts):
    return str(ts).split(" ")[0]


def write_mariadb(sdf, host, user, password, database, table, mode="append"):
    maria_properties = {
        "driver": "org.mariadb.jdbc.Driver",
        "user": user,
        "password": password,
    }
    maria_url = f"jdbc:mysql://{host}:3306/{database}?user={user}&password={password}"

    sdf.write.jdbc(url=maria_url, table=table, mode=mode, properties=maria_properties)
