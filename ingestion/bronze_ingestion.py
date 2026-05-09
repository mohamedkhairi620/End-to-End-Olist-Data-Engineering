import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .getOrCreate()

# القراءة من HDFS مباشرة
df_all = spark.read.json("/user/root/datalake/streaming")

# تقسيم حسب source_table
for table in ["customers", "orders", "order_items", "products"]:
    df_table = df_all.filter(df_all.source_table == table)
    count = df_table.count()
    if count > 0:
        df_table.write.mode("overwrite").parquet(f"/user/root/datalake/bronze/{table}")
        print(f" {table}: {count} records written to bronze")

spark.stop()
print("Bronze ingestion completed successfully!")