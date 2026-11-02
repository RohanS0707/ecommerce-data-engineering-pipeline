"""Bronze/Silver/Gold Spark ETL with optional PostgreSQL star-schema loading."""
import os
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from .schemas import CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA, ORDERS_SCHEMA, PAYMENTS_SCHEMA

def spark_session():
    builder=(SparkSession.builder.appName("ecommerce-medallion-pipeline")
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
            )
    # Avoid distributing an external JAR during Windows-only local runs. Spark's
    # Hadoop layer requires winutils.exe to stage downloaded files on Windows.
    if os.getenv("POSTGRES_HOST"):
        builder=builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
    return builder.getOrCreate()

def read_sources(spark, raw):
    raw=Path(raw)
    customers=spark.read.schema(CUSTOMERS_SCHEMA).option("header",True).csv(str(raw/"customers.csv"))
    products=spark.read.schema(PRODUCTS_SCHEMA).option("header",True).csv(str(raw/"products.csv"))
    orders=spark.read.schema(ORDERS_SCHEMA).json(str(raw/"orders.json"))
    payments=spark.read.schema(PAYMENTS_SCHEMA).json(str(raw/"payments.json"))
    return customers, products, orders, payments

def write_parquet(df, path, partitions=None):
    if partitions: df.write.mode("overwrite").partitionBy(*partitions).parquet(str(path))
    else: df.write.mode("overwrite").parquet(str(path))

def run_etl(spark, data_dir):
    root=Path(data_dir); raw=root/"raw"; bronze=root/"bronze"; silver=root/"silver"; gold=root/"gold"
    customers,products,orders,payments=read_sources(spark,raw)
    for name,df in [("customers",customers),("products",products),("orders",orders),("payments",payments)]: write_parquet(df,bronze/name)

    # Normalize keys/strings, then remove duplicate business keys deterministically.
    customers=(customers.dropDuplicates(["customer_id"])
               .withColumn("signup_date",F.to_date("signup_date")))
    products=products.dropDuplicates(["product_id"])
    product_keys=products.select("product_id").cache()
    customer_keys=customers.select("customer_id").cache()
    order_window=Window.partitionBy("order_id").orderBy(F.col("order_ts").desc_nulls_last())
    orders=(orders.withColumn("_rn",F.row_number().over(order_window))
            .withColumn("_is_duplicate",F.col("_rn")>1).drop("_rn")
            .withColumn("order_date",F.to_date("order_ts")))
    # Validation flags yield explainable rejected-record reports. Null/negative prices and missing keys reject.
    checked=(orders.join(F.broadcast(product_keys).withColumn("product_exists",F.lit(True)),"product_id","left")
             .join(customer_keys.withColumn("customer_exists",F.lit(True)),"customer_id","left")
             .withColumn("reject_reason", F.concat_ws(";",
                 F.when(F.col("order_id").isNull(),"null_order_id"),
                 F.when(F.col("customer_id").isNull(),"null_customer_id"),
                 F.when(F.col("product_id").isNull(),"null_product_id"),
                 F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0),"invalid_quantity"),
                 F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0),"invalid_price"),
                 F.when(F.col("_is_duplicate"),"duplicate_order"),
                 F.when(F.col("customer_exists").isNull(),"orphan_customer"),
                 F.when(F.col("product_exists").isNull(),"orphan_product")))
             .withColumn("is_valid",F.col("reject_reason")==""))
    valid=checked.filter("is_valid").drop("product_exists","customer_exists","reject_reason","is_valid","_is_duplicate")
    rejected=checked.filter("NOT is_valid").drop("product_exists","customer_exists","is_valid","_is_duplicate")
    valid=(valid.withColumn("gross_revenue",F.round(F.col("quantity")*F.col("unit_price"),2))
           .withColumn("net_revenue",F.round(F.col("quantity")*F.col("unit_price")*(1-F.coalesce("discount",F.lit(0.0))),2)))
    payments=payments.dropDuplicates(["payment_id"]).withColumn("payment_date",F.to_date("payment_ts"))
    # Payment rows whose order was rejected or absent are separately reported as orphans.
    orphan_payments=payments.join(valid.select("order_id"),"order_id","left_anti").withColumn("reject_reason",F.lit("orphan_or_rejected_order"))
    accepted_payments=payments.join(valid.select("order_id"),"order_id","inner")
    write_parquet(customers,silver/"customers"); write_parquet(products,silver/"products")
    write_parquet(valid,silver/"orders",["order_date"]); write_parquet(rejected,silver/"rejected_orders")
    write_parquet(accepted_payments,silver/"payments"); write_parquet(orphan_payments,silver/"rejected_payments")
    # Human-readable DQ evidence: one summary row per rejection class plus valid counts.
    order_report=(rejected.groupBy("reject_reason").count().withColumnRenamed("count","record_count").withColumn("dataset",F.lit("orders")))
    payment_report=(orphan_payments.groupBy("reject_reason").count().withColumnRenamed("count","record_count").withColumn("dataset",F.lit("payments")))
    accepted_report=spark.createDataFrame([("valid",valid.count(),"orders"),("valid",accepted_payments.count(),"payments")],"reject_reason string, record_count long, dataset string")
    dq=order_report.unionByName(payment_report).unionByName(accepted_report)
    dq.coalesce(1).write.mode("overwrite").option("header",True).csv(str(gold/"dq_report"))
    rejected.coalesce(1).write.mode("overwrite").option("header",True).csv(str(gold/"rejected_orders_csv"))
    orphan_payments.coalesce(1).write.mode("overwrite").option("header",True).csv(str(gold/"rejected_payments_csv"))

    # Gold dimensional model and analytics. Broadcast small dimensions; cache reusable curated orders.
    cust_for_join=customers.select("customer_id","first_name","last_name","email","signup_date")
    prod_for_join=products.select("product_id","product_name","category","cost")
    enriched=(valid.join(F.broadcast(cust_for_join),"customer_id").join(F.broadcast(prod_for_join),"product_id")
              .withColumn("date_key",F.date_format("order_date","yyyyMMdd").cast("int"))
              .withColumn("location_key",F.sha2(F.concat_ws("|","city","state","country"),256)))
    curated=enriched.repartition(max(2,spark.sparkContext.defaultParallelism),"order_date").cache()
    fact_orders=curated.select("order_id","customer_id","product_id","date_key","location_key","quantity","unit_price","discount","gross_revenue","net_revenue")
    pay_agg=accepted_payments.groupBy("order_id").agg(F.sum(F.when(F.lower("status")=="paid",F.col("amount")).otherwise(0)).alias("paid_amount"),F.count("payment_id").alias("payment_count"))
    fact_payments=(accepted_payments.join(pay_agg,"order_id").select("payment_id","order_id",F.col("payment_date").alias("date"),"amount","method","status","paid_amount","payment_count"))
    dim_customer=customers.select("customer_id","first_name","last_name","email","signup_date","city","state","country")
    dim_product=products.select("product_id","product_name","category","unit_price","cost")
    dim_date=(valid.select("order_date").distinct().withColumn("date_key",F.date_format("order_date","yyyyMMdd").cast("int"))
              .withColumn("year",F.year("order_date")).withColumn("quarter",F.quarter("order_date"))
              .withColumn("month",F.month("order_date")).withColumn("month_name",F.date_format("order_date","MMMM"))
              .withColumn("day",F.dayofmonth("order_date")).withColumn("day_of_week",F.date_format("order_date","EEEE")))
    dim_location=curated.select("location_key","city","state","country").distinct()
    customer_metrics=(fact_orders.groupBy("customer_id").agg(F.sum("net_revenue").alias("lifetime_value"),F.countDistinct("order_id").alias("order_count"),F.avg("net_revenue").alias("avg_order_value")))
    ranked=curated.withColumn("customer_order_rank",F.row_number().over(Window.partitionBy("customer_id").orderBy("order_ts")))
    daily=(curated.groupBy("order_date").agg(F.sum("net_revenue").alias("revenue"),F.countDistinct("order_id").alias("orders"))
           .withColumn("aov",F.when(F.col("orders")>0,F.col("revenue")/F.col("orders"))))
    tables={"fact_orders":fact_orders,"fact_payments":fact_payments,"dim_customer":dim_customer,"dim_product":dim_product,"dim_date":dim_date,"dim_location":dim_location,"customer_metrics":customer_metrics,"daily_revenue":daily,"customer_order_history":ranked.select("order_id","customer_id","order_ts","customer_order_rank")}
    for name,df in tables.items(): write_parquet(df,gold/name)
    # coalesce is used only for small single-file report output; repartition above supports scalable writes.
    curated.unpersist(); customer_keys.unpersist(); product_keys.unpersist()
    return tables

def load_postgres(tables):
    host=os.getenv("POSTGRES_HOST")
    if not host:
        print("POSTGRES_HOST is unset; Gold Parquet outputs are ready, skipping PostgreSQL load."); return
    url=f"jdbc:postgresql://{host}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','ecommerce')}"
    props={"user":os.getenv("POSTGRES_USER","ecommerce"),"password":os.getenv("POSTGRES_PASSWORD","ecommerce_dev"),"driver":"org.postgresql.Driver"}
    for name in ["dim_customer","dim_product","dim_date","dim_location","fact_orders","fact_payments"]:
        # Requires PostgreSQL JDBC jar in Spark classpath, supplied by spark.jars.packages in Docker and local run docs.
        tables[name].write.mode("overwrite").jdbc(url,name,properties=props)
        print(f"Loaded {name}")

def main():
    data_dir=os.getenv("DATA_DIR","data")
    spark=spark_session()
    try:
        tables=run_etl(spark,data_dir); load_postgres(tables)
        print(f"Pipeline complete. Gold outputs: {Path(data_dir)/'gold'}")
    finally: spark.stop()

if __name__=="__main__": main()

