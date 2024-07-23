from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("HiveConnectionTest")
    .config("hive.metastore.uris", "thrift://hive:9083")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("SHOW DATABASES").show()
