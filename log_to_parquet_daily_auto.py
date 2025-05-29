from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from kubernetes.client import V1ResourceRequirements

dag_name = "log_to_parquet_daily_auto"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=dag_name,
    default_args=default_args,
    start_date=datetime(2025, 5, 28),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["spark", "s3", "parquet"],
) as dag:

    @task()
    def run_spark_job(**context):
        return KubernetesPodOperator(
            task_id="run_spark_submit_s3_script_daily",
            name="spark-submit-s3-script-daily",
            namespace="airflow",
            image=spark_image,
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com",
                "--deploy-mode", "cluster",
                "--name", dag_name,
                "--conf", "spark.kubernetes.namespace=airflow",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=airflow-irsa",
                "--conf", "spark.kubernetes.container.image.pullSecrets=ecr-pull-secret",
                "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                "--conf", "spark.executor.instances=1",
                "--conf", "spark.executor.memory=512m",
                "--conf", "spark.executor.cores=1",
                "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
                "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
                "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
                "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
                "--conf", f"spark.kubernetes.container.image={spark_image}",
                "--conf", f"spark.kubernetes.driver.container.image={spark_image}",
                "s3a://creatz-airflow-jobs/monitoring/scripts/monitoring_logs_to_parquet_daily_v1.0.0.py",                
                "--start-date", context["ds"],
                "--end-date", context["ds"]
            ],
            get_logs=True,
            is_delete_operator_pod=True,
            service_account_name="airflow-irsa",
            image_pull_secrets=[k8s.V1LocalObjectReference(name="ecr-pull-secret")],
            container_resources=V1ResourceRequirements(
                requests={"memory": "1Gi", "cpu": "500m"},
                limits={"memory": "2Gi", "cpu": "1000m"},
            ),
            dag=dag,
        ).execute(context=context)

    run_spark_job()
