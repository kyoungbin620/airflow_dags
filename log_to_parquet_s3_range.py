from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="log_to_parquet_s3_range",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(default="2025-05-01", type="string", format="%Y-%m-%d", description="Start date"),
        "end_date": Param(default="2025-05-02", type="string", format="%Y-%m-%d", description="End date"),
    },
)
def log_to_parquet_dag():

    spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"

    spark_submit = KubernetesPodOperator(
        task_id="run_spark_submit_s3_script",
        name="spark-submit-s3-script",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com",
            "--deploy-mode", "cluster",
            "--name", "log_to_parquet_s3_range",
            "--conf", "spark.kubernetes.namespace=airflow",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=airflow-irsa",
            "--conf", "spark.kubernetes.container.image.pullSecrets=ecr-pull-secret",
            "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
            "--conf", "spark.executor.instances=1",
            "--conf", "spark.executor.memory=512m",
            "--conf", "spark.executor.cores=1",
            "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
            "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
            "--conf", f"spark.kubernetes.container.image={spark_image}",
            "--conf", f"spark.kubernetes.driver.container.image={spark_image}",
            "--conf", "spark.ui.proxyBase=/spark-ui/log_to_parquet_s3_range",
            "--conf", "spark.kubernetes.driver.label.spark-ui-selector=log_to_parquet_s3_range",
            "s3a://creatz-airflow-jobs/monitoring/scripts/monitoring_logs_to_parquet_daily_v1.0.0.py",
            "--start-date", "{{ params.start_date }}",
            "--end-date", "{{ params.end_date }}",
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        )
    )

    spark_submit

dag_instance = log_to_parquet_dag()
