from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_name    = "log_to_parquet_daily_auto"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server  = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Spark 설정 (기존 그대로)
spark_configs = {
    # 메모리 관리
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",
    # Executor 설정
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "2",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.executor.cores": "4",
    # 성능 최적화
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "20",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",
    "spark.sql.files.maxPartitionBytes": "134217728",
    "spark.default.parallelism": "8",
    "spark.sql.broadcastTimeout": "600",
    "spark.network.timeout": "800",
    # 동적 파티션 덮어쓰기 모드
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    # S3A 설정
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
    # 리소스 요청·제한
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores":   "2",
    "spark.kubernetes.executor.request.cores": "2",
    "spark.kubernetes.executor.limit.cores":   "4",
}

with DAG(
    dag_id=dag_name,
    default_args=default_args,
    # 매일 UTC 02:00 실행
    schedule_interval="0 2 * * *",
    # 고정된 UTC 과거 시작일 (pendulum 불필요)
    start_date=datetime(2025, 5, 28),
    catchup=False,
    tags=["spark", "s3", "parquet"],
) as dag:

    @task()
    def run_spark_job(**context):
        # Airflow 매크로 ds: "실행 기준일 YYYY-MM-DD"
        ds = context["ds"]                     # ex. "2025-06-10"
        prev_date = (                           # 전날 날짜 계산
            datetime.strptime(ds, "%Y-%m-%d")
            - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        # spark_configs → --conf 리스트
        spark_conf_args = []
        for key, value in spark_configs.items():
            spark_conf_args += ["--conf", f"{key}={value}"]

        # UI 프록시 추가 설정
        spark_conf_args += [
            "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
            "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
            "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
        ]

        return KubernetesPodOperator(
            task_id="run_spark_submit_s3_script_daily",
            name="spark-submit-s3-script-daily",
            namespace="airflow",
            image=spark_image,
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", api_server,
                "--deploy-mode", "cluster",
                "--name", dag_name,
                *spark_conf_args,
                "s3a://creatz-airflow-jobs/monitoring/scripts/monitoring_logs_to_parquet_daily_v1.0.0.py",
                "--start-date", prev_date,
                "--end-date",   prev_date,
            ],
            get_logs=True,
            is_delete_operator_pod=True,
            service_account_name="airflow-irsa",
            image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
            container_resources=V1ResourceRequirements(
                requests={"memory": "1Gi", "cpu": "500m"},
                limits={"memory": "2Gi", "cpu": "1000m"},
            ),
        ).execute(context=context)

    run_spark_job()
