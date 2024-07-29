import datetime
import logging
import os
import sys

from common import spark_session
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, expr, row_number
from pyspark.sql.types import DateType, Row, StructField, StructType
from pyspark.sql.window import Window

# Initialize Spark session
spark = spark_session(SparkSession)

# Configure log4j logging
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)


silver_path = "s3a://lakehouse/silver/"
gold_path = "s3a://lakehouse/gold/"


def log_schema_and_data(df, num_rows=5):
    df.printSchema()
    df.show(num_rows)


def create_dim_customer():
    try:
        customer = spark.read.format("delta").load(silver_path + "customers")
        geolocation = spark.read.format("delta").load(silver_path + "geolocation")

        # Perform the join operation
        joined_df = customer.join(
            geolocation,
            customer["customer_zip_code_prefix"]
            == geolocation["geolocation_zip_code_prefix"],
            how="left",
        )

        # Rename and select necessary columns
        joined_df = (
            joined_df.withColumnRenamed("geolocation_lat", "customer_lat")
            .withColumnRenamed("geolocation_lng", "customer_lng")
            .withColumn(
                "customer_city",
                coalesce(joined_df["geolocation_city"], joined_df["customer_city"]),
            )
            .withColumn(
                "customer_state",
                coalesce(joined_df["geolocation_state"], joined_df["customer_state"]),
            )
            .drop(
                "geolocation_city",
                "geolocation_state",
                "customer_zip_code_prefix",
                "geolocation_zip_code_prefix",
            )
            .dropDuplicates(subset=["customer_id"])
        )

        final_df = joined_df.select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "customer_lat",
            "customer_lng",
        )

        # Log schema and data
        log_schema_and_data(final_df)

        # Write the final DataFrame to the Delta table
        final_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "dim_customer")

        logger.info("Successfully created dim_customer table")
    except Exception as e:
        logger.error(f"Error creating dim_customer table: {e}")


def create_dim_seller():
    try:
        seller = spark.read.format("delta").load(silver_path + "sellers")

        final_df = seller.select("seller_id", "seller_zip_code_prefix")

        # Log schema and data
        log_schema_and_data(final_df)

        final_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "dim_seller")
        logger.info("Successfully created dim_seller table")
    except Exception as e:
        logger.error(f"Error creating dim_seller table: {e}")


def create_dim_review():
    try:
        review = spark.read.format("delta").load(silver_path + "order_reviews")

        final_df = review.select("review_id", "review_score")

        # Log schema and data
        log_schema_and_data(final_df)

        final_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "dim_review")
        logger.info("Successfully created dim_review table")
    except Exception as e:
        logger.error(f"Error creating dim_review table: {e}")


def create_dim_product():
    try:
        product = spark.read.format("delta").load(silver_path + "products")
        product_category = spark.read.format("delta").load(
            silver_path + "product_category"
        )

        joined_df = product.join(product_category, "product_category_name", "inner")

        final_df = joined_df.select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )

        # Log schema and data
        log_schema_and_data(final_df)

        final_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "dim_product")
        logger.info("Successfully created dim_product table")
    except Exception as e:
        logger.error(f"Error creating dim_product table: {e}")


def create_dim_order():
    try:
        order = spark.read.format("delta").load(silver_path + "orders")
        payment = spark.read.format("delta").load(silver_path + "payments")

        # Perform the join operation
        joined_df = order.join(payment, "order_id", "inner")

        # Select the necessary columns
        final_df = joined_df.select("order_id", "order_status", "payment_type")

        # Handle potential duplicates by taking the first payment type for each order
        window_spec = Window.partitionBy("order_id").orderBy("payment_sequential")
        final_df = (
            final_df.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        # Log schema and data
        log_schema_and_data(final_df)

        # Write the final DataFrame to the Delta table
        final_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "dim_order")

        logger.info("Successfully created dim_order table")
    except Exception as e:
        logger.error(f"Error creating dim_order table: {e}")


def create_dim_date():
    try:
        # Create a list of dates from 2017-01-01 to 2020-12-31
        start_date = datetime.date(2017, 1, 1)
        end_date = datetime.date(2020, 12, 31)
        date_list = [
            Row(
                full_date=datetime.datetime.combine(
                    start_date + datetime.timedelta(days=x), datetime.time()
                )
            )
            for x in range((end_date - start_date).days + 1)
        ]

        # Define schema for date DataFrame
        schema = StructType([StructField("full_date", DateType(), True)])

        # Create a DataFrame from the date list
        date_df = spark.createDataFrame(date_list, schema)

        # Add additional date-related columns
        date_df = (
            date_df.withColumn("year", expr("year(full_date)"))
            .withColumn("month", expr("month(full_date)"))
            .withColumn("day", expr("dayofmonth(full_date)"))
            .withColumn("day_of_week", expr("dayofweek(full_date)"))
            .withColumn("week_of_year", expr("weekofyear(full_date)"))
            .withColumn("quarter", expr("quarter(full_date)"))
            .withColumn(
                "is_weekend",
                expr("CASE WHEN dayofweek(full_date) IN (1, 7) THEN 1 ELSE 0 END"),
            )
        )

        # Log schema and data
        log_schema_and_data(date_df)

        # Save the DataFrame as a Delta table in the Gold layer
        date_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "dim_date")
        logger.info("Successfully created dim_date table")
    except Exception as e:
        logger.error(f"Error creating dim_date table: {e}")


def create_fact_table():
    try:
        # Load data from the Silver layer
        order = spark.read.format("delta").load(silver_path + "orders")
        order_items = spark.read.format("delta").load(silver_path + "order_items")
        payment = spark.read.format("delta").load(silver_path + "payments")
        review = spark.read.format("delta").load(silver_path + "order_reviews")

        # Load dimension data from the Gold layer
        customer = spark.read.format("delta").load(gold_path + "dim_customer")
        seller = spark.read.format("delta").load(gold_path + "dim_seller")
        product = spark.read.format("delta").load(gold_path + "dim_product")
        dim_date = spark.read.format("delta").load(gold_path + "dim_date")

        # Join orders with order items
        order_items_df = order.join(order_items, "order_id", "inner")

        # Join with payments
        payments_df = order_items_df.join(payment, "order_id", "inner")

        # Join with reviews
        reviews_df = payments_df.join(review, "order_id", "inner")

        # Join with customer dimension table
        customer_df = reviews_df.join(customer, "customer_id", "inner")

        # Join with seller dimension table
        seller_df = customer_df.join(seller, "seller_id", "inner")

        # Join with product dimension table
        product_df = seller_df.join(product, "product_id", "inner")

        # Join with date dimension table
        final_df = product_df.join(
            dim_date,
            product_df["order_purchase_timestamp"].cast(DateType())
            == dim_date["full_date"],
            "inner",
        )

        # Select relevant columns for the fact table
        fact_table = final_df.select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "review_id",
            "seller_id",
            "full_date",
            "price",
            "freight_value",
            "payment_value",
            "payment_installments",
            "payment_sequential",
        )

        # Log schema and data
        log_schema_and_data(fact_table)

        # Write the final DataFrame to the Delta table in the Gold layer
        fact_table.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "fact_table")

        logger.info("Successfully created fact_table")
    except Exception as e:
        logger.error(f"Error creating fact_table: {e}")


def create_data_mart():
    try:
        # Load data from the Gold layer
        fact_table = spark.read.format("delta").load(gold_path + "fact_table")
        dim_order = spark.read.format("delta").load(gold_path + "dim_order")
        dim_product = spark.read.format("delta").load(gold_path + "dim_product")
        dim_customer = spark.read.format("delta").load(gold_path + "dim_customer")
        dim_seller = spark.read.format("delta").load(gold_path + "dim_seller")
        dim_date = spark.read.format("delta").load(gold_path + "dim_date")

        # Join the fact table with dimension tables
        data_mart = (
            fact_table.join(dim_order, "order_id", "inner")
            .join(dim_product, "product_id", "inner")
            .join(dim_customer, "customer_id", "inner")
            .join(dim_seller, "seller_id", "inner")
            .join(dim_date, "full_date", "inner")
            .dropDuplicates(subset=["order_id", "customer_unique_id"])
        )

        # Log schema and data
        log_schema_and_data(data_mart)

        # Save the DataFrame as a Delta table in the Gold layer
        data_mart.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(gold_path + "data_mart")
        logger.info("Successfully created data_mart")
    except Exception as e:
        logger.error(f"Error creating data_mart: {e}")


# Create dimension and fact tables
create_dim_customer()
create_dim_seller()
create_dim_review()
create_dim_product()
create_dim_order()
create_dim_date()
create_fact_table()
create_data_mart()
