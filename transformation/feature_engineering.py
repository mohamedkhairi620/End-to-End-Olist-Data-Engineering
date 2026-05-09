import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, to_timestamp
os.environ["HADOOP_USER_NAME"] = "root"
# ============================================
# Spark Session with LOCAL mode (clean)
# ============================================
spark = SparkSession.builder \
    .appName("FeatureEngineering") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .getOrCreate()

print("=" * 60)
print("STARTING FEATURE ENGINEERING")
print("=" * 60)

# ============================================
# HDFS Paths
# ============================================
BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze"
CLEAN_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/clean"

# ============================================
# 1. CUSTOMERS
# ============================================
customers = spark.read.parquet(f"{BRONZE_PATH}/customers")
customers_clean = customers.dropDuplicates(["customer_id"])
customers_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/customers")
print(f"✅ customers cleaned: {customers_clean.count()} records")

# ============================================
# 2. ORDERS
# ============================================
orders = spark.read.parquet(f"{BRONZE_PATH}/orders")
orders_clean = orders.dropDuplicates(["order_id"])

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for c in date_columns:
    if c in orders_clean.columns:
        orders_clean = orders_clean.withColumn(c, to_timestamp(col(c)))

orders_clean = orders_clean.join(customers_clean.select("customer_id"), "customer_id", "inner")
orders_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/orders")
print(f"✅ orders cleaned: {orders_clean.count()} records")

# ============================================
# 3. ORDER ITEMS
# ============================================
order_items = spark.read.parquet(f"{BRONZE_PATH}/order_items")
order_items_clean = order_items.dropDuplicates(["order_id", "order_item_id"])

if "price" in order_items_clean.columns:
    mean_price = order_items_clean.select(mean(col("price"))).collect()[0][0]
    order_items_clean = order_items_clean.withColumn(
        "price",
        when(col("price").isNull(), mean_price).otherwise(col("price"))
    )

order_items_clean = order_items_clean.join(orders_clean.select("order_id"), "order_id", "inner")
order_items_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/order_items")
print(f"✅ order_items cleaned: {order_items_clean.count()} records")

# ============================================
# 4. PRODUCTS
# ============================================
products = spark.read.parquet(f"{BRONZE_PATH}/products")
products_clean = products.dropDuplicates(["product_id"])

if "product_category_name" in products_clean.columns:
    products_clean = products_clean.withColumn(
        "product_category_name",
        when(col("product_category_name").isNull(), "unknown").otherwise(col("product_category_name"))
    )

products_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/products")
print(f"✅ products cleaned: {products_clean.count()} records")

print("\n✅ FEATURE ENGINEERING COMPLETED SUCCESSFULLY")
spark.stop()