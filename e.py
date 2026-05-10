import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("NorthwindBatchExtractOnly") \
    .config("spark.ui.enabled", "false") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.driver.host", "172.31.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

LANDING_ZONE = "file:///opt/airflow/data/raw_northwind_pings/"
BRONZE_BASE = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/"

tables = [
    {"name": "orders", "pattern": "orders_*.json", "output": "northwind_orders"},
    {"name": "order_details", "pattern": "order_details_*.json", "output": "northwind_order_details"},
    {"name": "customers", "pattern": "customers_*.json", "output": "northwind_customers"},
    {"name": "employees", "pattern": "employees_*.json", "output": "northwind_employees"},
    {"name": "products", "pattern": "products_*.json", "output": "northwind_products"},
    {"name": "categories", "pattern": "categories_*.json", "output": "northwind_categories"},
    {"name": "suppliers", "pattern": "suppliers_*.json", "output": "northwind_suppliers"},
    {"name": "shippers", "pattern": "shippers_*.json", "output": "northwind_shippers"},
]


def ingest_table(table):
    table_name = table["name"]
    input_path = LANDING_ZONE + table["pattern"]
    output_path = BRONZE_BASE + table["output"] + "/"

    try:
        df = spark.read \
            .option("mode", "PERMISSIVE") \
            .json(input_path)
    except AnalysisException:
        print(f"{table_name}: no JSON files found. Skipping.")
        return 0

    df = df.withColumn("source_file", F.input_file_name()) \
           .withColumn("bronze_ingestion_time", F.current_timestamp())

    if df.count() == 0:
        print(f"{table_name}: no rows found. Skipping.")
        return 0

    try:
        existing_bronze = spark.read.parquet(output_path)

        if "source_file" in existing_bronze.columns:
            processed_files = existing_bronze.select("source_file").distinct()

            df = df.join(
                processed_files,
                on="source_file",
                how="left_anti"
            )

    except AnalysisException:
        pass

    new_rows = df.count()

    if new_rows == 0:
        print(f"{table_name}: no new files to ingest. Skipping write.")
        return 0

    df.write \
        .mode("append") \
        .format("parquet") \
        .save(output_path)

    print(f"{table_name}: ingested {new_rows} new rows.")
    return new_rows


try:
    print("=" * 70)
    print("STARTING NORTHWIND EXTRACT TO BRONZE")
    print("=" * 70)

    total_new_rows = 0

    for table in tables:
        total_new_rows += ingest_table(table)

    print("=" * 70)

    if total_new_rows == 0:
        print("EXTRACT FINISHED: no new files were ingested.")
    else:
        print(f"EXTRACT FINISHED: {total_new_rows} new rows ingested to Bronze.")

    print("=" * 70)

except Exception as e:
    print(f"EXTRACT FAILED: {e}")
    raise

finally:
    spark.stop()