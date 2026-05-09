import os
from pyspark.sql import SparkSession
import configparser

os.environ["HADOOP_USER_NAME"] = "root"

config = configparser.ConfigParser()
config.read("/home/jovyan/work/connections.my_example_connection")

account = config['connections.my_example_connection']['account'].strip('"')
user = config['connections.my_example_connection']['user'].strip('"')
password = config['connections.my_example_connection']['password'].strip('"')
role = config['connections.my_example_connection']['role'].strip('"')
warehouse = config['connections.my_example_connection']['warehouse'].strip('"')
database = config['connections.my_example_connection']['database'].strip('"')
schema = config['connections.my_example_connection']['schema'].strip('"')

sf_options = {
    "sfURL": f"{account}.snowflakecomputing.com",
    "sfUser": user,
    "sfPassword": password,
    "sfRole": role,
    "sfWarehouse": warehouse,
    "sfDatabase": database,
    "sfSchema": schema
}

spark = SparkSession.builder \
    .appName('LoadToSnowflake') \
    .master('local[*]') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .getOrCreate()

print("=" * 60)
print("SPARK CONNECTED SUCCESSFULLY (Local Mode)")
print("=" * 60)

GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold"

def load_table(table_name, mode="overwrite"):
    print(f"\n📤 Loading {table_name}...")
    df = spark.read.parquet(f"{GOLD_BASE_PATH}/{table_name}")
    count = df.count()
    print(f"   Records: {count}")
    
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name.upper()) \
        .mode(mode) \
        .save()
    
    print(f"✅ {table_name} loaded")

try:
    load_table("dim_customer", "overwrite")
    load_table("dim_product", "overwrite")
    load_table("dim_order_items", "overwrite")     
    load_table("fact_orders", "overwrite")
    
    print("\n🎉 ALL TABLES LOADED TO SNOWFLAKE SUCCESSFULLY!")
except Exception as e:
    print(f"\n❌ LOADING FAILED: {e}")
finally:
    spark.stop()