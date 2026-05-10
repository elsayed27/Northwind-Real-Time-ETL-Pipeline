from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# ============================================================
# CONFIG
# ============================================================

SPARK_CONTAINER = "spark-jupyter"
SPARK_JOBS_DIR = "/home/jovyan/spark_jobs"

LANDING_ZONE = "/opt/airflow/data/raw_northwind_pings"

SNOWFLAKE_JARS = (
    "/home/jovyan/spark_jobs/jars/snowflake-jdbc-3.13.30.jar,"
    "/home/jovyan/spark_jobs/jars/spark-snowflake_2.12-2.11.1-spark_3.3.jar"
)

HDFS_BRONZE_PATH = "/user/root/datalake/bronze"
HDFS_SILVER_PATH = "/user/root/datalake/silver"
HDFS_GOLD_PATH = "/user/root/datalake/gold"


default_args = {
    "owner": "ali",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def docker_exec(command: str) -> str:
    return f'docker exec {SPARK_CONTAINER} bash -lc "{command}"'


def spark_submit_command(script_name: str, use_snowflake_jars: bool = False) -> str:
    if use_snowflake_jars:
        return docker_exec(
            f"cd {SPARK_JOBS_DIR} && "
            f"spark-submit --jars {SNOWFLAKE_JARS} {script_name}"
        )

    return docker_exec(
        f"cd {SPARK_JOBS_DIR} && spark-submit {script_name}"
    )
#     schedule="*/2 * * * *",


with DAG(
    dag_id="northwind_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    end_date=datetime(2030, 12, 31),
    tags=["northwind", "etl", "spark", "hdfs", "snowflake"],
) as dag:

    print_start = BashOperator(
        task_id="print_start",
        bash_command='echo "Starting Northwind ETL Pipeline at: $(date)"',
    )

    check_docker_cli = BashOperator(
        task_id="check_docker_cli",
        bash_command=(
            "echo 'Checking Docker CLI inside Airflow Scheduler...' && "
            "which docker && "
            "docker --version && "
            "docker ps"
        ),
    )

    check_spark_container = BashOperator(
        task_id="check_spark_container",
        bash_command=docker_exec(
            "echo 'Spark container is working' && "
            "hostname && "
            "pwd && "
            "ls -l /home/jovyan/spark_jobs"
        ),
    )

    check_source_files = BashOperator(
        task_id="check_source_files",
        bash_command=docker_exec(
            f"echo 'Checking Landing Zone JSON files...' && "
            f"ls -lah {LANDING_ZONE} || true && "
            f"find {LANDING_ZONE} -type f -name '*.json' | head"
        ),
    )

    extract_to_bronze = BashOperator(
        task_id="extract_to_bronze",
        bash_command=spark_submit_command("e.py"),
    )
    clean_to_silver = BashOperator(
        task_id="clean_to_silver",
        bash_command=spark_submit_command("cleaning.py"),
    )
    transform_to_gold = BashOperator(
        task_id="transform_to_gold",
        bash_command=spark_submit_command("t.py"),
    )

    verify_gold_layer = BashOperator(
        task_id="verify_gold_layer",
        bash_command=docker_exec(
            f"hdfs dfs -ls {HDFS_GOLD_PATH}"
        ),
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command=spark_submit_command("l.py", use_snowflake_jars=True),
    )

    print_done = BashOperator(
        task_id="print_done",
        bash_command='echo "Northwind ETL Pipeline finished successfully at: $(date)"',
    )

    (
        print_start
        >> check_docker_cli
        >> check_spark_container
        >> check_source_files
        >> extract_to_bronze
        >> clean_to_silver
        >> transform_to_gold
        >> verify_gold_layer
        >> load_to_snowflake
        >> print_done
    )
