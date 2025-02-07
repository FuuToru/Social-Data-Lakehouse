import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2, unix_timestamp, explode
from utils import spark_session
from pyspark.sql.functions import *



def process_bronze_to_silver(spark: SparkSession, year: int, month: int):
    # Define paths
    bronze_path = f"s3a://lakehouse/bronze/{year}/{month:02d}"
    silver_path = f"s3a://lakehouse/silver/{year}/{month:02d}/"

    # Load the raw data
    bronze_df = spark.read.format("json").load(bronze_path)
    
    # Làm sạch và chuẩn hóa dữ liệu
    silver_df = bronze_df.select(
        col("social_type"),
        col("media_channel"),
        col("created_at"),
        col("keyword"),
        col("brand"),
        col("user_name"),
        col("user_id"),
        col("title"),
        col("url"),
        col("view"),
        col("reaction.like").alias("likes"),
        col("comment_count"),
        explode(col("comments")).alias("comment")
    ).select(
        col("social_type"),
        col("media_channel"),
        col("created_at"),
        col("keyword"),
        col("brand"),
        col("user_name"),
        col("user_id"),
        col("title"),
        col("url"),
        col("view"),
        col("likes"),
        col("comment_count"),
        col("comment.created_at").alias("comment_created_at"),
        col("comment.user_name").alias("comment_user_name"),
        col("comment.content").alias("comment_content"),
        col("comment.reaction.like").alias("comment_likes")
    )
    # Save the processed data in the Silver layer as Delta
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process bronze data to silver")
    parser.add_argument("--year", type=int, required=True, help="Year of the data")
    parser.add_argument("--month", type=int, required=True, help="Month of the data")

    args = parser.parse_args()
    spark = spark_session()
    process_bronze_to_silver(spark, args.year, args.month)
