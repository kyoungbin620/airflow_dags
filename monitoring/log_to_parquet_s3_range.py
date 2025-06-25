from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_id = "log_to_parquet_s3_range"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Spark 설정 중앙 관리
spark_configs = {
    # 메모리 관리
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",

    # Executor 설정
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "1",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.executor.cores": "1",

    # 성능 최적화
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "1",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",
    "spark.sql.files.maxPartitionBytes": "134217728",
    "spark.default.parallelism": "1",
    "spark.sql.broadcastTimeout": "600",
    "spark.network.timeout": "800",

    # 동적 파티션
    "spark.sql.sources.partitionOverwriteMode": "dynamic",

    # S3 설정
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
    "spark.hadoop.fs.s3a.access.style": "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

    # Kubernetes 설정
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.kubernetes.container.image": spark_image,
    "spark.kubernetes.driver.container.image": spark_image,
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",

    # 리소스 요청/제한
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores": "2",
    "spark.kubernetes.executor.request.cores": "1",
    "spark.kubernetes.executor.limit.cores": "2",
    "spark.kubernetes.executor.node.selector.intent": "spark",
}

@dag(
    dag_id=dag_id,
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

    # spark_configs → --conf로 변환
    spark_conf_args = []
    for key, value in spark_configs.items():
        spark_conf_args.extend(["--conf", f"{key}={value}"])

    # UI proxy 관련 conf 추가
    spark_conf_args.extend([
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_id}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_id}",
        "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
    ])

    spark_submit = KubernetesPodOperator(
        task_id="run_spark_submit_s3_script",
        name="spark-submit-s3-script",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        node_selector={"intent": "spark"},
        arguments=[
            "--master", api_server,
            "--deploy-mode", "cluster",
            "--name", dag_id,
            *spark_conf_args,
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
