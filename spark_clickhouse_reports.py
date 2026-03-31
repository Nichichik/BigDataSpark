from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_ClickHouse_Reports") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2,com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.3.1") \
    .getOrCreate()


pg_url = "jdbc:postgresql://postgres_db:5432/lab_db"
pg_props = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://172.20.1.3:8123/reports_db?compression=none"
ch_props = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "use_lz4": "false",
    "compress": "0"
}

fact_sales = spark.read.jdbc(url=pg_url, table="fact_sales", properties=pg_props)
dim_products = spark.read.jdbc(url=pg_url, table="dim_products", properties=pg_props)
dim_customers = spark.read.jdbc(url=pg_url, table="dim_customers", properties=pg_props)
dim_stores = spark.read.jdbc(url=pg_url, table="dim_stores", properties=pg_props)
dim_suppliers = spark.read.jdbc(url=pg_url, table="dim_suppliers", properties=pg_props)


report_products = fact_sales.join(dim_products, "product_id") \
    .groupBy("product_name", "product_category") \
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("sale_quantity").alias("total_items_sold"),
        F.avg("product_rating").alias("average_rating")
    ).orderBy(F.desc("total_revenue"))

spark.sql(f"DROP TABLE IF EXISTS reports_db.report_products").collect()
report_products.write.jdbc(url=ch_url, table="report_products", mode="append", properties=ch_props) # mode="append"


report_customers = fact_sales.join(dim_customers, "customer_id") \
    .groupBy("customer_first_name", "customer_last_name", "customer_country") \
    .agg(
        F.sum("sale_total_price").alias("total_spent"),
        F.count("sale_key").alias("orders_count"),
        (F.sum("sale_total_price") / F.count("sale_key")).alias("average_check")
    ).orderBy(F.desc("total_spent"))

spark.sql(f"DROP TABLE IF EXISTS reports_db.report_customers").collect()
report_customers.write.jdbc(url=ch_url, table="report_customers", mode="append", properties=ch_props) # mode="append"


report_time = fact_sales \
    .withColumn("sale_year", F.year("sale_date")) \
    .withColumn("sale_month", F.month("sale_date")) \
    .groupBy("sale_year", "sale_month") \
    .agg(
        F.sum("sale_total_price").alias("monthly_revenue"),
        F.count("sale_key").alias("total_monthly_orders"),
        F.avg("sale_total_price").alias("avg_order_size")
    ).orderBy("sale_year", "sale_month")

spark.sql(f"DROP TABLE IF EXISTS reports_db.report_time").collect()
report_time.write.jdbc(url=ch_url, table="report_time", mode="append", properties=ch_props) # mode="append"


report_stores = fact_sales.join(dim_stores, "store_id") \
    .groupBy("store_name", "store_city", "store_country") \
    .agg(
        F.sum("sale_total_price").alias("store_revenue"),
        F.avg("sale_total_price").alias("store_avg_check"),
        F.sum("sale_quantity").alias("store_items_sold")
    ).orderBy(F.desc("store_revenue"))

spark.sql(f"DROP TABLE IF EXISTS reports_db.report_stores").collect()
report_stores.write.jdbc(url=ch_url, table="report_stores", mode="append", properties=ch_props) # mode="append"


report_suppliers = fact_sales.join(dim_suppliers, "supplier_id") \
    .groupBy("supplier_name", "supplier_country") \
    .agg(
        F.sum("sale_total_price").alias("supplier_revenue"),
        (F.sum("sale_total_price") / F.sum("sale_quantity")).alias("avg_item_price")
    ).orderBy(F.desc("supplier_revenue"))

spark.sql(f"DROP TABLE IF EXISTS reports_db.report_suppliers").collect()
report_suppliers.write.jdbc(url=ch_url, table="report_suppliers", mode="append", properties=ch_props) # mode="append"


report_quality = fact_sales.join(dim_products, "product_id") \
    .groupBy("product_name") \
    .agg(
        F.avg("product_rating").alias("avg_rating"),
        F.sum("sale_quantity").alias("total_sales_volume")
    ).orderBy(F.desc("avg_rating"))

spark.sql(f"DROP TABLE IF EXISTS reports_db.report_quality").collect()
report_quality.write.jdbc(url=ch_url, table="report_quality", mode="append", properties=ch_props) # mode="append"

spark.stop()
