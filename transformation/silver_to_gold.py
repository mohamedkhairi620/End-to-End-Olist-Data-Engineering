import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

# ============================================
# Spark Session with LOCAL mode
# ============================================
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .getOrCreate()

print("=" * 60)
print("BUILDING GOLD LAYER - STAR SCHEMA")
print("=" * 60)

# ============================================
# Paths
# ============================================
CLEAN_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/clean"
GOLD_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold"

# ============================================
# Load Clean Data
# ============================================
customers = spark.read.parquet(f"{CLEAN_PATH}/customers")
orders = spark.read.parquet(f"{CLEAN_PATH}/orders")
order_items = spark.read.parquet(f"{CLEAN_PATH}/order_items")
products = spark.read.parquet(f"{CLEAN_PATH}/products")

print(f"✅ Customers loaded: {customers.count()} records")
print(f"✅ Orders loaded: {orders.count()} records")
print(f"✅ Order_items loaded: {order_items.count()} records")
print(f"✅ Products loaded: {products.count()} records")

# ============================================
# 1. DIM_CUSTOMER
# ============================================
dim_customer = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
).dropDuplicates(["customer_id"])

dim_customer.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customer")
print(f"✅ dim_customer: {dim_customer.count()} records")

# ============================================
# 2. DIM_PRODUCT
# ============================================
dim_product = products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).dropDuplicates(["product_id"])

dim_product.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_product")
print(f"✅ dim_product: {dim_product.count()} records")

# ============================================
# 3. DIM_ORDER_ITEMS 
# ============================================
dim_order_items = order_items.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
).dropDuplicates(["order_id", "order_item_id"])

dim_order_items.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_order_items")
print(f"✅ dim_order_items: {dim_order_items.count()} records")

# ============================================
# 4. FACT_ORDERS
# ============================================
fact_orders = order_items.join(orders, "order_id", "inner") \
    .select(
        order_items["order_id"],
        orders["customer_id"],
        order_items["product_id"],
        order_items["seller_id"],
        order_items["price"],
        order_items["freight_value"],
        orders["order_status"],
        orders["order_purchase_timestamp"]
    )

fact_orders.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_orders")
print(f"✅ fact_orders: {fact_orders.count()} records")

print("\n🎉 GOLD LAYER (STAR SCHEMA) CREATED SUCCESSFULLY")
spark.stop()