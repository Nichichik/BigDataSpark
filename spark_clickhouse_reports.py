from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_ClickHouse_Reports") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres_db:5432/lab_db"
pg_props = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://clickhouse-db:8123/reports_db"
ch_props = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver"
}

fact_sales = spark.read.jdbc(url=pg_url, table="fact_sales", properties=pg_props)
dim_products = spark.read.jdbc(url=pg_url, table="dim_products", properties=pg_props)
dim_customers = spark.read.jdbc(url=pg_url, table="dim_customers", properties=pg_props)
dim_stores = spark.read.jdbc(url=pg_url, table="dim_stores", properties=pg_props)
dim_suppliers = spark.read.jdbc(url=pg_url, table="dim_suppliers", properties=pg_props)

def execute_ch_sql(sql):
    conn = spark._jvm.java.sql.DriverManager.getConnection(ch_url, "spark_user", "spark_password")
    stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
    conn.close()

def save_and_view(df, table_name, view_definitions):
    df.write.jdbc(url=ch_url, table=table_name, mode="overwrite", properties=ch_props)
    for view_name, query in view_definitions.items():
        execute_ch_sql(f"DROP VIEW IF EXISTS {view_name}")
        execute_ch_sql(f"CREATE VIEW {view_name} AS {query}")

report_products = fact_sales.join(dim_products, "product_id") \
    .groupBy("product_name", "product_category") \
    .agg(F.sum("sale_total_price").alias("total_revenue"),
         F.sum("sale_quantity").alias("total_items_sold"),
         F.avg("product_rating").alias("average_rating"),
         F.count("sale_key").alias("total_reviews_count"))

products_views = {
    "v1_top_10_sold_products": "SELECT product_name, total_items_sold FROM report_products ORDER BY total_items_sold DESC LIMIT 10",
    "v2_revenue_by_category": "SELECT product_category, sum(total_revenue) as revenue FROM report_products GROUP BY product_category",
    "v3_product_rating_reviews": "SELECT product_name, average_rating, total_reviews_count FROM report_products"
}
save_and_view(report_products, "report_products", products_views)

report_customers = fact_sales.join(dim_customers, "customer_id") \
    .groupBy("customer_first_name", "customer_last_name", "customer_country") \
    .agg(F.sum("sale_total_price").alias("total_spent"),
         F.count("sale_key").alias("orders_count"),
         F.avg("sale_total_price").alias("average_check"))

customers_views = {
    "v4_top_10_customers_spent": "SELECT customer_first_name, customer_last_name, total_spent FROM report_customers ORDER BY total_spent DESC LIMIT 10",
    "v5_customers_distribution_country": "SELECT customer_country, count(*) as client_count FROM report_customers GROUP BY customer_country",
    "v6_customer_average_check": "SELECT customer_first_name, customer_last_name, average_check FROM report_customers"
}
save_and_view(report_customers, "report_customers", customers_views)

report_time = fact_sales \
    .withColumn("sale_year", F.year("sale_date")) \
    .withColumn("sale_month", F.month("sale_date")) \
    .groupBy("sale_year", "sale_month") \
    .agg(F.sum("sale_total_price").alias("monthly_revenue"),
         F.avg("sale_total_price").alias("avg_order_size"))

time_views = {
    "v7_monthly_yearly_trends": "SELECT sale_year, sale_month, monthly_revenue FROM report_time ORDER BY sale_year, sale_month",
    "v8_revenue_comparison": "SELECT sale_month, sum(monthly_revenue) as total_revenue FROM report_time GROUP BY sale_month",
    "v9_avg_order_size_month": "SELECT sale_month, avg(avg_order_size) FROM report_time GROUP BY sale_month"
}
save_and_view(report_time, "report_time", time_views)

report_stores = fact_sales.join(dim_stores, "store_id") \
    .groupBy("store_name", "store_city", "store_country") \
    .agg(F.sum("sale_total_price").alias("store_revenue"),
         F.avg("sale_total_price").alias("store_avg_check"))

stores_views = {
    "v10_top_5_stores_revenue": "SELECT store_name, store_revenue FROM report_stores ORDER BY store_revenue DESC LIMIT 5",
    "v11_stores_by_location": "SELECT store_country, store_city, sum(store_revenue) FROM report_stores GROUP BY store_country, store_city",
    "v12_store_avg_check": "SELECT store_name, store_avg_check FROM report_stores"
}
save_and_view(report_stores, "report_stores", stores_views)

report_suppliers = fact_sales.join(dim_suppliers, "supplier_id") \
    .groupBy("supplier_name", "supplier_country") \
    .agg(F.sum("sale_total_price").alias("supplier_revenue"),
         F.avg("sale_total_price").alias("avg_item_price"))

suppliers_views = {
    "v13_top_5_suppliers_revenue": "SELECT supplier_name, supplier_revenue FROM report_suppliers ORDER BY supplier_revenue DESC LIMIT 5",
    "v14_supplier_avg_price": "SELECT supplier_name, avg_item_price FROM report_suppliers",
    "v15_suppliers_by_country": "SELECT supplier_country, count(*) FROM report_suppliers GROUP BY supplier_country"
}
save_and_view(report_suppliers, "report_suppliers", suppliers_views)

report_quality = fact_sales.join(dim_products, "product_id") \
    .groupBy("product_name") \
    .agg(F.avg("product_rating").alias("avg_rating"),
         F.sum("sale_quantity").alias("total_sales_volume"),
         F.count("sale_key").alias("review_count"))

quality_views = {
    "v16_product_rating_extremes": "SELECT product_name, avg_rating FROM report_quality ORDER BY avg_rating DESC",
    "v17_rating_sales_correlation": "SELECT product_name, avg_rating, total_sales_volume FROM report_quality",
    "v18_products_most_reviews": "SELECT product_name, review_count FROM report_quality ORDER BY review_count DESC"
}
save_and_view(report_quality, "report_quality", quality_views)

print("SUCCESS: 18 VIEWS CREATED")
spark.stop()