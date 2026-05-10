import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

os.environ["HADOOP_USER_NAME"] = "root"

# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("NorthwindSilverToGoldTransformation") \
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

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
SILVER_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/silver/"
GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

# ─────────────────────────────────────────────
# Read Silver Tables
# ─────────────────────────────────────────────
orders = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_orders"
)

order_details = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_order_details"
)

customers = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_customers"
)

employees = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_employees"
)

products = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_products"
)

categories = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_categories"
)

suppliers = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_suppliers"
)

shippers = spark.read.parquet(
    SILVER_BASE_PATH + "northwind_shippers"
)

print("All Silver tables loaded successfully.")

# ─────────────────────────────────────────────
# DIM_DATES
# ─────────────────────────────────────────────
print("\nCreating dim_dates...")

date_df = orders.select(
    F.to_date("OrderDate").alias("date")
).distinct()

dim_dates = date_df.withColumn(
    "date_key",
    F.date_format("date", "yyyyMMdd").cast("int")
).withColumn(
    "year",
    F.year("date")
).withColumn(
    "month",
    F.month("date")
).withColumn(
    "day",
    F.dayofmonth("date")
).withColumn(
    "day_name",
    F.date_format("date", "EEEE")
).withColumn(
    "day_of_week",
    F.dayofweek("date")
).withColumn(
    "week_of_year",
    F.weekofyear("date")
).withColumn(
    "quarter",
    F.quarter("date")
).withColumn(
    "is_weekend",
    F.dayofweek("date").isin([1, 7])
)

dim_dates.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "dim_dates")

print("dim_dates written to Gold Layer.")

# ─────────────────────────────────────────────
# DIM_CUSTOMERS
# ─────────────────────────────────────────────
print("\nCreating dim_customers...")

dim_customers = customers.select(
    "CustomerID",
    "CompanyName",
    "ContactName",
    "ContactTitle",
    "Address",
    "City",
    "Region",
    "PostalCode",
    "Country",
    "Phone",
    "Fax"
)

dim_customers.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "dim_customers")

print("dim_customers written to Gold Layer.")

# ─────────────────────────────────────────────
# DIM_EMPLOYEES
# ─────────────────────────────────────────────
print("\nCreating dim_employees...")

dim_employees = employees.select(
    "EmployeeID",
    "Title",
    "TitleOfCourtesy",
    "BirthDate",
    "HireDate",
    "Address",
    "City",
    "Region",
    "PostalCode",
    "Country",
    "HomePhone",
    "Extension",
    "ReportsTo"
).withColumn(
    "source_system",
    F.lit("Northwind")
)

dim_employees.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "dim_employees")

print("dim_employees written to Gold Layer.")

# ─────────────────────────────────────────────
# DIM_PRODUCTS
# ─────────────────────────────────────────────
print("\nCreating dim_products...")

dim_products = products.alias("p").join(
    categories.alias("c"),
    F.col("p.CategoryID") == F.col("c.CategoryID"),
    "left"
).select(
    F.col("p.ProductID"),
    F.col("p.ProductName"),
    F.col("p.SupplierID"),
    F.col("p.CategoryID"),
    F.col("c.CategoryName"),
    F.col("c.Description").alias("CategoryDescription"),
    F.col("p.QuantityPerUnit"),
    F.col("p.UnitPrice").alias("ProductListPrice"),
    F.col("p.UnitsInStock"),
    F.col("p.UnitsOnOrder"),
    F.col("p.ReorderLevel"),
    F.col("p.Discontinued")
).withColumn(
    "source_system",
    F.lit("Northwind")
)

dim_products.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "dim_products")

print("dim_products written to Gold Layer.")

# ─────────────────────────────────────────────
# DIM_SUPPLIERS
# ─────────────────────────────────────────────
print("\nCreating dim_suppliers...")

dim_suppliers = suppliers.select(
    F.col("SupplierID"),
    F.col("CompanyName").alias("SupplierName"),
    F.col("ContactName"),
    F.col("ContactTitle"),
    F.col("Address"),
    F.col("City"),
    F.col("Region"),
    F.col("PostalCode"),
    F.col("Country"),
    F.col("Phone"),
    F.col("Fax"),
    F.col("HomePage")
).withColumn(
    "source_system",
    F.lit("Northwind")
)

dim_suppliers.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "dim_suppliers")

print("dim_suppliers written to Gold Layer.")

# ─────────────────────────────────────────────
# DIM_SHIPPERS
# ─────────────────────────────────────────────
print("\nCreating dim_shippers...")

dim_shippers = shippers.select(
    F.col("ShipperID"),
    F.col("CompanyName").alias("ShipperName"),
    F.col("Phone")
)

dim_shippers.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "dim_shippers")

print("dim_shippers written to Gold Layer.")

# ─────────────────────────────────────────────
# FACT_ORDERS
# ─────────────────────────────────────────────
print("\nCreating fact_orders...")

fact_orders = order_details.alias("od").join(
    orders.alias("o"),
    "OrderID",
    "inner"
).join(
    products.alias("p"),
    "ProductID",
    "left"
).select(
    F.col("OrderID"),
    F.col("ProductID"),
    F.col("p.SupplierID"),
    F.col("o.CustomerID"),
    F.col("o.EmployeeID"),
    F.col("o.ShipVia").alias("ShipperID"),

    F.date_format(
        F.to_date("o.OrderDate"),
        "yyyyMMdd"
    ).cast("int").alias("date_key"),

    F.col("od.UnitPrice"),
    F.col("od.Quantity"),
    F.col("od.Discount"),

    (
        F.col("od.UnitPrice") *
        F.col("od.Quantity") *
        (1 - F.col("od.Discount"))
    ).alias("TotalAmount"),

    F.col("o.Freight"),
    F.col("o.OrderDate"),
    F.col("o.RequiredDate"),
    F.col("o.ShippedDate")

).withColumn(
    "gold_created_at",
    F.current_timestamp()
)

fact_orders.write \
    .mode("overwrite") \
    .parquet(GOLD_BASE_PATH + "fact_orders")

print("fact_orders written to Gold Layer.")

# ─────────────────────────────────────────────
# Final Summary
# ─────────────────────────────────────────────
print("\n" + "=" * 80)
print("GOLD STAR SCHEMA CREATED SUCCESSFULLY")
print("=" * 80)

print(f"dim_dates       : {dim_dates.count()}")
print(f"dim_customers   : {dim_customers.count()}")
print(f"dim_employees   : {dim_employees.count()}")
print(f"dim_products    : {dim_products.count()}")
print(f"dim_suppliers   : {dim_suppliers.count()}")
print(f"dim_shippers    : {dim_shippers.count()}")
print(f"fact_orders     : {fact_orders.count()}")

spark.stop()
print("\nSpark Session Stopped.")
