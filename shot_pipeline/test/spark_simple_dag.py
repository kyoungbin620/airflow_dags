from airflow import DAG
from datetime import datetime
from astronomer.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="spark_simple_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkKubernetesOperator(
        task_id="run_simple_spark_job",
        namespace="airflow",  # Spark driver pod이 실행될 namespace
        application_file="/opt/airflow/dags/repo/shot_pipeline/spark-simple.yaml",  # YAML 위치
        do_xcom_push=False,
    )
