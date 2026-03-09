"""
Medallion Lakehouse — Master DAG
Orchestrates: UPI batch + CC stream → Bronze → Silver → dbt Gold → Postgres export
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SPARK_MASTER = "spark://spark-master:7077"
SPARK_JARS = "/opt/spark/jars/delta-spark.jar,/opt/spark/jars/delta-storage.jar,/opt/spark/jars/spark-kafka.jar,/opt/spark/jars/postgresql.jar"
DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
}

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="End-to-end Medallion pipeline: UPI + CC → Bronze → Silver → Gold → Postgres",
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "bronze", "silver", "gold"],
) as dag:

    # ── Bronze: UPI Batch ─────────────────────────────────────────────────
    bronze_upi = SparkSubmitOperator(
        task_id="bronze_upi_ingest",
        application="/opt/spark/jobs/bronze_upi_ingest.py",
        conn_id="spark_default",
        executor_memory="1G",
        driver_memory="1G",
        jars=SPARK_JARS,
        conf=DELTA_CONF,
        name="bronze-upi-ingest",
    )

    # ── Bronze: CC Streaming (one micro-batch) ────────────────────────────
    bronze_cc = SparkSubmitOperator(
        task_id="bronze_cc_ingest",
        application="/opt/spark/jobs/bronze_cc_stream_ingest.py",
        conn_id="spark_default",
        executor_memory="1G",
        driver_memory="1G",
        jars=SPARK_JARS,
        conf=DELTA_CONF,
        name="bronze-cc-ingest",
    )

    # ── Silver: UPI Transform ─────────────────────────────────────────────
    silver_upi = SparkSubmitOperator(
        task_id="silver_upi_transform",
        application="/opt/spark/jobs/silver_upi_transform.py",
        conn_id="spark_default",
        executor_memory="1G",
        driver_memory="1G",
        jars=SPARK_JARS,
        conf=DELTA_CONF,
        name="silver-upi-transform",
    )

    # ── Silver: CC Transform ──────────────────────────────────────────────
    silver_cc = SparkSubmitOperator(
        task_id="silver_cc_transform",
        application="/opt/spark/jobs/silver_cc_transform.py",
        conn_id="spark_default",
        executor_memory="1G",
        driver_memory="1G",
        jars=SPARK_JARS,
        conf=DELTA_CONF,
        name="silver-cc-transform",
    )

    # ── dbt Gold Layer ────────────────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id="dbt_gold_run",
        bash_command="""
        docker exec dbt dbt run --profiles-dir /dbt/profiles --project-dir /dbt --target prod
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_gold_test",
        bash_command="""
        docker exec dbt dbt test --profiles-dir /dbt/profiles --project-dir /dbt --target prod
        """,
    )

    # ── Pipeline complete signal ──────────────────────────────────────────
    done = BashOperator(
        task_id="pipeline_complete",
        bash_command='echo "Pipeline run complete at $(date)"',
    )

    # ── DAG Dependency graph ──────────────────────────────────────────────
    [bronze_upi, bronze_cc] >> [silver_upi, silver_cc] >> dbt_gold >> dbt_test >> done