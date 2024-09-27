import os

from pyspark.sql import SparkSession

# AWS Credentials and configuration for MinIO
AWS_ACCESS_KEY = "minio"
AWS_SECRET_KEY = "minio123"
AWS_S3_ENDPOINT = "http://minio:9000"
AWS_BUCKET_NAME = "lakehouse"


def spark_session():
    """
    Create and configure a Spark session for interacting with the MinIO S3 storage
    and Hive Metastore. Delta Lake integration is also enabled.

    Returns:
        SparkSession: Configured Spark session.
    """
    spark = (
        SparkSession.builder.appName("Ingest checkin table into bronze")
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
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/s3-2.18.41.jar,"
            "/opt/spark/jars/aws-java-sdk-1.12.367.jar,"
            "/opt/spark/jars/delta-core_2.12-2.4.0.jar,"
            "/opt/spark/jars/delta-storage-2.4.0.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,",
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    # Set Spark logging level
    spark.sparkContext.setLogLevel("INFO")

    return spark
