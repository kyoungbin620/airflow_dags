from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_name = "base_to_swingdata_range"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

spark_configs = {
    # 메모리
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",
    "spark.driver.maxResultSize": "512m",

    # Executor
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "1",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.executor.cores": "1",

    # 성능 최적화
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "20",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",
    "spark.sql.files.maxPartitionBytes": "134217728",
    "spark.default.parallelism": "4",
    "spark.sql.broadcastTimeout": "600",
    "spark.network.timeout": "800",

    # 동적 파티션 관리
    "spark.sql.sources.partitionOverwriteMode": "dynamic",

    # S3
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
    "spark.hadoop.fs.s3a.access.style": "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

    # Kubernetes
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.kubernetes.driver.container.image": spark_image,
    "spark.kubernetes.container.image": spark_image,
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores": "2",
    "spark.kubernetes.executor.request.cores": "1",
    "spark.kubernetes.executor.limit.cores": "2",
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(default="2025-05-01", type="string"),
        "end_date": Param(default="2025-05-02", type="string"),
    },
    tags=["spark", "s3", "parquet"],
)
def raw_to_parquet_dag():

    @task()
    def log_inputs(start_date: str, end_date: str):
        import logging
        logging.info(f"[INPUT] start_date: {start_date}")
        logging.info(f"[INPUT] end_date: {end_date}")

    log_task = log_inputs("{{ params.start_date }}", "{{ params.end_date }}")

    arguments = [
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", dag_name,
    ]

    for key, value in spark_configs.items():
        arguments += ["--conf", f"{key}={value}"]

    arguments += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
        "s3a://creatz-airflow-jobs/monitoring/scripts/run_swingdata_extract_pipeline_v1.0.0.py",
        "--start-date", "{{ params.start_date }}",
        "--end-date", "{{ params.end_date }}",
    ]

    spark_submit = KubernetesPodOperator(
        task_id="run_raw_to_parquet_range",
        name="base-to-shotinfo_ai-pipeline",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=arguments,
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    log_task >> spark_submit

dag_instance = raw_to_parquet_dag()
