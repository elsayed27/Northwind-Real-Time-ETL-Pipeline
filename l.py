import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("NorthwindSnowflakeLoader") \
    .config("spark.ui.enabled", "false") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.31.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

sf_options = {
    "sfURL": "ya91010.eu-central-2.aws.snowflakecomputing.com",
    "sfUser": "AliAlshaikh",
    "sfPassword": "YbdgbLisPzcFbT8",
    "sfDatabase": "NORTHWIND_DW",
    "sfSchema": "GOLD_LAYER",
    "sfWarehouse": "NORTHWIND_WH",
}

TABLES = {
    "dim_dates": "DIM_DATES",
    "dim_customers": "DIM_CUSTOMERS",
    "dim_employees": "DIM_EMPLOYEES",
    "dim_products": "DIM_PRODUCTS",
    "dim_suppliers": "DIM_SUPPLIERS",
    "dim_shippers": "DIM_SHIPPERS",
    "fact_orders": "FACT_ORDERS",
}


def load_table(hdfs_table_name, snowflake_table_name):
    input_path = GOLD_BASE_PATH + hdfs_table_name

    print("\n" + "=" * 80)
    print(f"Loading HDFS Gold table: {hdfs_table_name}")
    print(f"To Snowflake table     : {snowflake_table_name}")
    print(f"Input path             : {input_path}")

    df = spark.read.parquet(input_path)

    row_count = df.count()
    print(f"Rows to load: {row_count}")

    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", snowflake_table_name) \
        .mode("overwrite") \
        .save()

    print(f"Overwritten {snowflake_table_name} with {row_count} rows")


try:
    print("=" * 80)
    print("STARTING LOAD FROM HDFS GOLD TO SNOWFLAKE")
    print("=" * 80)

    for hdfs_table_name, snowflake_table_name in TABLES.items():
        load_table(hdfs_table_name, snowflake_table_name)

    print("\nALL GOLD TABLES OVERWRITTEN SUCCESSFULLY IN SNOWFLAKE")

except Exception as e:
    print(f"Loading failed: {e}")
    raise

finally:
    spark.stop()
    print("Spark Session Stopped.")
