from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_name = "run_raw_to_parquet_hour"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(default="2025-05-01", type="string", format="%Y-%m-%d", description="시작 날짜"),
        "end_date": Param(default="2025-05-02", type="string", format="%Y-%m-%d", description="종료 날짜"),
        "hour": Param(default=None, type=["null", "string"], description="특정 시간 (00-23, 미지정시 전체 시간)"),
    },
    tags=["spark", "s3", "parquet"],
)
def raw_to_parquet_dag():
    arguments = [
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
        "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
        "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
        "--conf", f"spark.kubernetes.container.image={spark_image}",
        "--conf", f"spark.kubernetes.driver.container.image={spark_image}",
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
        "--py-files", "s3a://creatz-airflow-jobs/shot_data/zips/raw_to_parquet_v1.0.0.zip",
        "main.py",  # 이건 zip 안에 들어있는 이름 그대로
        "--start-date", "{{ params.start_date }}",
        "--end-date", "{{ params.end_date }}",
    ]

    

    # hour 파라미터가 지정된 경우에만 추가
    if "{{ params.hour }}" != "None":
        arguments += ["--hour", "{{ params.hour }}"]

    spark_submit = KubernetesPodOperator(
        task_id="run_raw_to_parquet_hour",
        name="raw-to-parquet-pipeline",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=arguments,
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        )
    )

    spark_submit

dag_instance = raw_to_parquet_dag()
