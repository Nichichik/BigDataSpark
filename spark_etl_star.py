from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Star_Schema") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres_db:5432/lab_db"
pg_properties = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}

raw_df = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_properties)

raw_df = raw_df.withColumn("parsed_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))


dim_customers = raw_df.dropDuplicates(["sale_customer_id"]).select(
    F.col("sale_customer_id").alias("customer_id"),
    "customer_first_name", "customer_last_name", "customer_country"
)

dim_products = raw_df.dropDuplicates(["sale_product_id"]).select(
    F.col("sale_product_id").alias("product_id"),
    "product_name", "product_category", "product_price", "product_rating"
)

dim_stores = raw_df.dropDuplicates(["store_name"]).select(
    "store_name", "store_city", "store_country"
).withColumn("store_id", F.monotonically_increasing_id())

dim_suppliers = raw_df.dropDuplicates(["supplier_name"]).select(
    "supplier_name", "supplier_country"
).withColumn("supplier_id", F.monotonically_increasing_id())


fact_sales = raw_df \
    .join(dim_stores.select("store_name", "store_id"), "store_name", "left") \
    .join(dim_suppliers.select("supplier_name", "supplier_id"), "supplier_name", "left") \
    .select(
        F.monotonically_increasing_id().alias("sale_key"),
        F.col("parsed_date").alias("sale_date"),
        F.col("sale_customer_id").alias("customer_id"),
        F.col("sale_product_id").alias("product_id"),
        F.col("store_id"),
        F.col("supplier_id"),
        F.col("sale_quantity").cast("int"),
        F.col("sale_total_price").cast("decimal(18,2)")
    )


dim_customers.write.jdbc(url=pg_url, table="dim_customers", mode="overwrite", properties=pg_properties)
dim_products.write.jdbc(url=pg_url, table="dim_products", mode="overwrite", properties=pg_properties)
dim_stores.write.jdbc(url=pg_url, table="dim_stores", mode="overwrite", properties=pg_properties)
dim_suppliers.write.jdbc(url=pg_url, table="dim_suppliers", mode="overwrite", properties=pg_properties)
fact_sales.write.jdbc(url=pg_url, table="fact_sales", mode="overwrite", properties=pg_properties)

spark.stop()
