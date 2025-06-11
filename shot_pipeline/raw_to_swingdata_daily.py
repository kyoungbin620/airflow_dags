from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

# === 공통 설정 ===
dag_name = "raw_to_swingdata_daily"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# === Spark 공통 구성 ===
spark_configs = {
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "1",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.executor.cores": "1",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "20",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",
    "spark.sql.files.maxPartitionBytes": "134217728",
    "spark.default.parallelism": "4",
    "spark.sql.broadcastTimeout": "600",
    "spark.network.timeout": "800",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
    "spark.hadoop.fs.s3a.access.style": "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.kubernetes.container.image": spark_image,
    "spark.kubernetes.driver.container.image": spark_image,
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores": "2",
    "spark.kubernetes.executor.request.cores": "1",
    "spark.kubernetes.executor.limit.cores": "2",
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="0 17 * * *",  # UTC 기준 매일 17시에 실 행
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "s3", "parquet"],
)
def raw_to_swingdata_range_dag():

    @task()
    def get_run_date(**context):
        ds = context["ds"]
        prev_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        return prev_date

    run_date = get_run_date()

    @task()
    def log_date(run_date: str):
        import logging
        logging.info(f"[PROCESS DATE] {run_date}")

    log_task = log_date(run_date)

    # Spark 공통 conf -> arguments 변환
    common_conf = []
    for key, value in spark_configs.items():
        common_conf += ["--conf", f"{key}={value}"]
    common_conf += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ]

    # Raw -> Base 처리
    def make_args(script_path, run_date_str):
        return [
            "--master", api_server,
            "--deploy-mode", "cluster",
            "--name", f"{dag_name}-raw" if "raw" in script_path else f"{dag_name}-base",
            *common_conf,
            "--py-files", "s3a://creatz-airflow-jobs/raw_to_parquet/zips/dependencies.zip" if "raw" in script_path else None,
            script_path,
            "--start-date", run_date_str,
            "--end-date", run_date_str,
        ]

    raw_task = KubernetesPodOperator(
        task_id="run_raw_to_base_range",
        name="raw-to-base-pipeline",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=make_args("s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py", "{{ ti.xcom_pull(task_ids='get_run_date') }}"),
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    base_task = KubernetesPodOperator(
        task_id="run_base_to_swingdata_range",
        name="base-to-swingdata-pipeline",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=make_args("s3a://creatz-airflow-jobs/monitoring/scripts/run_swingdata_extract_pipeline_v1.0.0.py", "{{ ti.xcom_pull(task_ids='get_run_date') }}"),
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    get_run_date() >> log_task >> raw_task >> base_task

dag = raw_to_swingdata_range_dag()