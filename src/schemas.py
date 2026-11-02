"""Explicit input contracts for the synthetic CSV and JSON sources."""
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, TimestampType)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False), StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True), StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True), StructField("city", StringType(), True),
    StructField("state", StringType(), True), StructField("country", StringType(), True),
])
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False), StructField("product_name", StringType(), True),
    StructField("category", StringType(), True), StructField("unit_price", DoubleType(), True),
    StructField("cost", DoubleType(), True),
])
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False), StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True), StructField("order_ts", TimestampType(), True),
    StructField("quantity", IntegerType(), True), StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True), StructField("city", StringType(), True),
    StructField("state", StringType(), True), StructField("country", StringType(), True),
])
PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), False), StructField("order_id", StringType(), True),
    StructField("payment_ts", TimestampType(), True), StructField("amount", DoubleType(), True),
    StructField("method", StringType(), True), StructField("status", StringType(), True),
])

