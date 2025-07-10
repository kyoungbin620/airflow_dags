from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator



with DAG(
    dag_id="spark_simple_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkKubernetesOperator(
        task_id="run_simple_spark_job",
        namespace="airflow",  # SparkApplication이 생성될 namespace
        application_file="repo/shot_pipeline/test/spark-simple.yaml",
        do_xcom_push=False,
    )
