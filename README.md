# Northwind Real-Time ETL Pipeline
## Description

Northwind Real-Time ETL Pipeline built using PySpark, Hadoop HDFS, Apache Airflow, Docker, and Snowflake. The project simulates real-time transaction data, processes it through Bronze, Silver, and Gold layers, performs validation and cleaning, builds a Star Schema, and loads analytical tables into Snowflake for reporting and analytics.

## Overview

This project implements a complete end-to-end Data Engineering ETL pipeline for the Northwind dataset using:

- Python
- PySpark
- Hadoop HDFS
- Apache Airflow
- Docker
- Snowflake
- Parquet

The pipeline simulates real-time order transactions from CSV files, writes them as JSON batches into a Landing Zone, ingests the raw data into the Bronze Layer in HDFS, validates and cleans the data into the Silver Layer, transforms it into a Gold Star Schema, and finally loads the analytical tables into Snowflake.

---

# Full Workflow

```text
CSV Files
   ↓
simulateRealWorldData.ipynb
   ↓
Landing Zone (JSON batches)
   ↓
e.py
   ↓
Bronze Layer (HDFS)
   ↓
validation.py
   ↓
cleaning.py
   ↓
Silver Layer (HDFS)
   ↓
s_validation.py
   ↓
t.py
   ↓
Gold Star Schema (HDFS)
   ↓
l.py
   ↓
Snowflake Data Warehouse
```

---

# Architecture

```text
Northwind CSV Files
        ↓
Simulator Notebook
        ↓
JSON Landing Zone
        ↓
Bronze Layer
        ↓
Bronze Validation
        ↓
Cleaning
        ↓
Silver Layer
        ↓
Silver Validation
        ↓
Gold Star Schema
        ↓
Snowflake
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| PySpark | Distributed data processing |
| Hadoop HDFS | Distributed storage system |
| Apache Airflow | Workflow orchestration |
| Docker | Containerized environment |
| Snowflake | Cloud Data Warehouse |
| Jupyter Notebook | Data simulation and testing |

---

# Project Structure

```text
bigdataproject/
├── docker-compose.yml
├── README.md
├── dags/
│   └── etl_dag.py
├── notebooks/
│   ├── simulateRealWorldData.ipynb
│   ├── e.py
│   ├── validation.py
│   ├── cleaning.py
│   ├── s_validation.py
│   ├── t.py
│   └── l.py
├── data/
│   ├── raw_northwind_pings/
│   └── Northwind CSV files
├── jars/
│   ├── snowflake-jdbc-3.13.30.jar
│   └── spark-snowflake_2.12-2.11.1-spark_3.3.jar
└── logs/
```

---

# Important Docker Volume Mappings

```text
./dags      → /opt/airflow/dags
./data      → /opt/airflow/data
./notebooks → /home/jovyan/spark_jobs
./jars      → /home/jovyan/spark_jobs/jars
./logs      → /opt/airflow/logs
```

The Airflow DAG does not run PySpark directly inside the Airflow container. 
Instead, Airflow runs Spark jobs inside the Spark/Jupyter container using:

```bash
docker exec spark-jupyter bash -lc "cd /home/jovyan/spark_jobs && spark-submit script_name.py"
```

---

# Main Containers

The project uses several Docker containers:

```text
airflow-webserver
airflow-scheduler
airflow-triggerer
postgres_airflow
spark-jupyter
hadoop-namenode
hadoop-datanode1
hadoop-datanode2
resourcemanager
hadoop-nodemanager
hadoop-nodemanager2
```

---

# Important URLs

| Service | URL |
|---|---|
| Airflow UI | http://localhost:18080 |
| Jupyter Notebook | http://localhost:8899 |
| Spark UI | http://localhost:4040 |
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Snowflake | https://app.snowflake.com |

Airflow default login:

```text
Username: airflow
Password: airflow
```

---

# Step 1 — Simulate Real-Time Data

File:

```text
simulateRealWorldData.ipynb
```

This notebook simulates real-time data arrival.

It reads Northwind CSV files and creates small JSON batches, then writes them into the Landing Zone.

Landing Zone:

```text
/opt/airflow/data/raw_northwind_pings/
```

The purpose of this step is to make the project look like a real data pipeline where data arrives continuously in small batches instead of all at once.

---

# Step 2 — Extract to Bronze Layer

File:

```text
e.py
```

Purpose:

- Reads raw JSON files from the Landing Zone.
- Adds metadata columns such as source file and ingestion time.
- Avoids reprocessing already ingested files using `source_file`.
- Writes the raw data into HDFS Bronze Layer in Parquet format.

Input:

```text
file:///opt/airflow/data/raw_northwind_pings/
```

Output:

```text
hdfs://hadoop-namenode:9000/user/root/datalake/bronze/
```

Bronze tables:

```text
northwind_orders
northwind_order_details
northwind_customers
northwind_employees
northwind_products
northwind_categories
northwind_suppliers
northwind_shippers
```

The Bronze Layer stores the raw data as received, with minimal transformation.

---

# Step 3 — Bronze Validation

File:

```text
validation.py
```

Purpose:

This script checks the quality of the raw Bronze data before cleaning.

It checks:

- Required columns
- Empty columns
- Null or empty values
- Fully null business rows
- Duplicate business keys

This step only reports issues.  
It does not clean or modify the data.

---

# Step 4 — Cleaning and Silver Layer

File:

```text
cleaning.py
```

Purpose:

This script cleans the Bronze data and writes the cleaned version into the Silver Layer.

Main cleaning operations:

- Removes fully empty rows.
- Renames inconsistent columns.
- Converts columns to correct data types.
- Trims string values.
- Handles missing regions by replacing them with `Unknown`.
- Removes duplicate records using business keys.
- Drops unnecessary empty columns when needed.

Example:

In the raw orders data, the source column may be:

```text
Customer key
```

The cleaning script standardizes it to:

```text
CustomerID
```

Output:

```text
hdfs://hadoop-namenode:9000/user/root/datalake/silver/
```

Silver tables:

```text
northwind_orders
northwind_order_details
northwind_customers
northwind_employees
northwind_products
northwind_categories
northwind_suppliers
northwind_shippers
```

The Silver Layer contains clean, standardized, analysis-ready data.

---

# Step 5 — Silver Validation

File:

```text
s_validation.py
```

Purpose:

This script validates the cleaned Silver data.

It checks:

- Required fields after cleaning
- Duplicate business keys
- Fully null business rows
- Empty tables
- Remaining null or empty values

If the Silver validation passes, the data is ready for transformation into the Gold Layer.

---

# Step 6 — Transform to Gold Star Schema

File:

```text
t.py
```

Purpose:

This script reads cleaned Silver tables and transforms them into a Gold Star Schema.

Input:

```text
hdfs://hadoop-namenode:9000/user/root/datalake/silver/
```

Output:

```text
hdfs://hadoop-namenode:9000/user/root/datalake/gold/
```

Final Gold tables:

```text
dim_dates
dim_customers
dim_employees
dim_products
dim_suppliers
dim_shippers
fact_orders
```

---

# Gold Tables Explanation

## dim_dates

Stores date information extracted from order dates.

Columns include:

```text
date_key
date
year
month
day
day_name
day_of_week
week_of_year
quarter
is_weekend
```

---

## dim_customers

Stores customer information.

Main columns:

```text
CustomerID
CompanyName
ContactName
ContactTitle
Address
City
Region
PostalCode
Country
Phone
Fax
```

---

## dim_employees

Stores employee information.

Main columns:

```text
EmployeeID
Title
TitleOfCourtesy
BirthDate
HireDate
Address
City
Region
PostalCode
Country
HomePhone
Extension
ReportsTo
source_system
```

---

## dim_products

Stores product information with category details.

This table is created by joining products with categories.

Main columns:

```text
ProductID
ProductName
SupplierID
CategoryID
CategoryName
CategoryDescription
QuantityPerUnit
ProductListPrice
UnitsInStock
UnitsOnOrder
ReorderLevel
Discontinued
source_system
```

---

## dim_suppliers

Stores supplier information.

Main columns:

```text
SupplierID
SupplierName
ContactName
ContactTitle
Address
City
Region
PostalCode
Country
Phone
Fax
HomePage
source_system
```

---

## dim_shippers

Stores shipping company information.

Main columns:

```text
ShipperID
ShipperName
Phone
```

---

## fact_orders

Stores the measurable business transactions.

This is the main fact table.

It combines:

- orders
- order_details
- products

Main columns:

```text
OrderID
ProductID
SupplierID
CustomerID
EmployeeID
ShipperID
date_key
UnitPrice
Quantity
Discount
TotalAmount
Freight
OrderDate
RequiredDate
ShippedDate
gold_created_at
```

The `TotalAmount` is calculated as:

```text
UnitPrice * Quantity * (1 - Discount)
```

---

# Step 7 — Load to Snowflake

File:

```text
l.py
```

Purpose:

This script loads the Gold tables from HDFS into Snowflake.

Input:

```text
hdfs://hadoop-namenode:9000/user/root/datalake/gold/
```

Snowflake target:

```text
Database: NORTHWIND_DW
Schema: GOLD_LAYER
Warehouse: NORTHWIND_WH
```

Loaded Snowflake tables:

```text
DIM_DATES
DIM_CUSTOMERS
DIM_EMPLOYEES
DIM_PRODUCTS
DIM_SUPPLIERS
DIM_SHIPPERS
FACT_ORDERS
```

The script reads each Gold parquet table and writes it to Snowflake using the Snowflake Spark connector.

---

# Apache Airflow Orchestration

Airflow is used to automate the full ETL pipeline.

DAG file:

```text
dags/etl_dag.py
```

DAG name:

```text
northwind_etl_pipeline
```

The DAG runs Spark scripts inside the `spark-jupyter` container using `docker exec`.

---

# Airflow DAG Workflow

```text
print_start
   ↓
check_docker_cli
   ↓
check_spark_container
   ↓
check_source_files
   ↓
extract_to_bronze
   ↓
clean_to_silver
   ↓
transform_to_gold
   ↓
verify_gold_layer
   ↓
load_to_snowflake
   ↓
print_done
```

---

# Airflow Task Details

## 1 — print_start

Prints the pipeline start timestamp.

```bash
echo "Starting Northwind ETL Pipeline at: $(date)"
```

---

## 2 — check_docker_cli

Checks that the Airflow Scheduler can access Docker.

```bash
docker ps
```

This is important because Airflow runs Spark jobs using Docker commands.

---

## 3 — check_spark_container

Checks that the Spark/Jupyter container is reachable.

```bash
docker exec spark-jupyter bash -lc "ls -l /home/jovyan/spark_jobs"
```

---

## 4 — check_source_files

Checks if JSON files exist in the Landing Zone.

```bash
find /opt/airflow/data/raw_northwind_pings -type f -name '*.json'
```

---

## 5 — extract_to_bronze

Runs:

```bash
spark-submit e.py
```

This ingests JSON files into the Bronze Layer.

---

## 6 — clean_to_silver

Runs:

```bash
spark-submit cleaning.py
```

This cleans Bronze data and writes Silver data.

---

## 7 — transform_to_gold

Runs:

```bash
spark-submit t.py
```

This builds the Gold Star Schema.

---

## 8 — verify_gold_layer

Checks that the Gold Layer exists in HDFS.

```bash
hdfs dfs -ls /user/root/datalake/gold
```

Expected folders:

```text
dim_dates
dim_customers
dim_employees
dim_products
dim_suppliers
dim_shippers
fact_orders
```

---

## 9 — load_to_snowflake

Runs:

```bash
spark-submit --jars snowflake-jdbc-3.13.30.jar,spark-snowflake_2.12-2.11.1-spark_3.3.jar l.py
```

This loads the Gold tables into Snowflake.

---

## 10 — print_done

Prints the pipeline completion timestamp.

```bash
echo "Northwind ETL Pipeline finished successfully"
```

---

# How to Run the Project

## 1. Start Docker Services

From the project root:

```bash
cd /home/ali-alshaikh/Desktop/bigdataproject
docker-compose up -d
```

---

## 2. Check Containers

```bash
docker ps
```

Important containers should be running:

```text
airflow-webserver
airflow-scheduler
airflow-triggerer
postgres_airflow
spark-jupyter
hadoop-namenode
hadoop-datanode1
hadoop-datanode2
resourcemanager
hadoop-nodemanager
hadoop-nodemanager2
```

---

## 3. Open Jupyter Notebook

```text
http://localhost:8899
```

Run:

```text
simulateRealWorldData.ipynb
```

This generates JSON batches in the Landing Zone.

---

## 4. Open Airflow UI

```text
http://localhost:18080
```

Login:

```text
Username: airflow
Password: airflow
```

Enable the DAG:

```text
DAGs → northwind_etl_pipeline → Unpause
```

Run manually:

```text
Trigger DAG
```

Monitor execution from:

```text
Grid View
Graph View
Logs
```

---

# Snowflake Setup

## Create Database, Schema, and Warehouse

Inside Snowflake, run:

```sql
CREATE DATABASE IF NOT EXISTS NORTHWIND_DW;

CREATE SCHEMA IF NOT EXISTS NORTHWIND_DW.GOLD_LAYER;

CREATE WAREHOUSE IF NOT EXISTS NORTHWIND_WH
WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
```

---

# Snowflake Tables

Create the target tables in Snowflake before loading, or allow Spark to create/overwrite them depending on connector behavior.

Main target tables:

```text
DIM_DATES
DIM_CUSTOMERS
DIM_EMPLOYEES
DIM_PRODUCTS
DIM_SUPPLIERS
DIM_SHIPPERS
FACT_ORDERS
```

---

# Snowflake Connection Configuration

Inside `l.py`, Snowflake options are configured like this:

```python
sf_options = {
    "sfURL": "YOUR_ACCOUNT.eu-central-2.aws.snowflakecomputing.com",
    "sfUser": "YOUR_USERNAME",
    "sfPassword": "YOUR_PASSWORD",
    "sfDatabase": "NORTHWIND_DW",
    "sfSchema": "GOLD_LAYER",
    "sfWarehouse": "NORTHWIND_WH",
}
```

---

# Verify Snowflake Load

Inside Snowflake, run:

```sql
SELECT COUNT(*) FROM FACT_ORDERS;
```

You can also check:

```sql
SELECT COUNT(*) FROM DIM_CUSTOMERS;
SELECT COUNT(*) FROM DIM_PRODUCTS;
SELECT COUNT(*) FROM DIM_DATES;
```

---

# Reset Project for Demo

## Delete Bronze Layer

```bash
hdfs dfs -rm -r /user/root/datalake/bronze/*
```

---

## Delete Silver Layer

```bash
hdfs dfs -rm -r /user/root/datalake/silver/*
```

---

## Delete Gold Layer

```bash
hdfs dfs -rm -r /user/root/datalake/gold/*
```

---

## Delete Landing Zone JSON Files

```bash
rm -rf /opt/airflow/data/raw_northwind_pings/*
```

---

# Final Result

After the pipeline runs successfully:

1. JSON batches are created from Northwind CSV files.
2. Raw data is ingested into HDFS Bronze.
3. Bronze data is validated.
4. Data is cleaned and written to Silver.
5. Silver data is validated.
6. Gold Star Schema tables are built.
7. Gold tables are loaded into Snowflake.
8. Airflow controls the workflow automatically.

This project demonstrates a complete modern Data Engineering pipeline using Docker, Airflow, Spark, Hadoop HDFS, and Snowflake.
