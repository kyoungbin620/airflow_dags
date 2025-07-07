from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import timedelta

dag_name = "raw_to_swingdata_daily_test"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="0 1 * * *",  # 매일 UTC 1시 실행
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "k8s", "s3"],
) as dag:

    raw_to_base = SparkKubernetesOperator(
        task_id="run_raw_to_base_daily",
        namespace="airflow",
        application_file="/opt/airflow/dags/repo/shot_pipeline/raw-to-base.yaml",
        kubernetes_conn_id="kubernetes_default",
        delete_on_termination=True,
        reattach_on_restart=True,
        get_logs=True,
    )

    base_to_swing = SparkKubernetesOperator(
        task_id="run_base_to_swingdata_daily",
        namespace="airflow",
        application_file="/opt/airflow/dags/repo/shot_pipeline/base-to-swing.yaml",
        kubernetes_conn_id="kubernetes_default",
        delete_on_termination=True,
        reattach_on_restart=True,
        get_logs=True,
    )

    swing_to_db = SparkKubernetesOperator(
        task_id="run_spark_shot_summary_daily",
        namespace="airflow",
        application_file="/opt/airflow/dags/repo/shot_pipeline/swing-to-db.yaml",
        kubernetes_conn_id="kubernetes_default",
        delete_on_termination=True,
        reattach_on_restart=True,
        get_logs=True,
    )

    raw_to_base >> base_to_swing >> swing_to_db
