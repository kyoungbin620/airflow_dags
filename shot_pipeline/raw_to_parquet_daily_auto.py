from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

# DAG 이름과 Spark 이미지, API 서버 주소 정의
dag_name    = "raw_to_parquet_daily_auto"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server  = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

# 기본 인자 설정
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2025, 6, 10, 17, 0),
}

# Spark 파라미터 맵 (기존 설정 그대로 사용)
spark_configs = {
    # 메모리 관리
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",

    # Executor 설정 (코어 1개)
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

    # 드라이버/익스큐터 리소스 요청·제한
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores":   "2",
    "spark.kubernetes.executor.request.cores": "1",
    "spark.kubernetes.executor.limit.cores":   "2",
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    # 매일 KST 02:00에 실행
    schedule_interval="0 17 * * *",
    # 과거 태스크는 건너뜀
    catchup=False,
    # 시작일을 고정된 과거 날짜로 설정
    start_date=datetime(2025, 5, 1),
    tags=["spark", "s3", "parquet"],
)
def raw_to_parquet_dag():

    @task()
    def log_inputs(run_date: str):
        import logging
        logging.info(f"[INPUT] 처리 대상 날짜: {run_date}")

    # ───────────────────────────────────────────────────────
    # 실행일(ds)이 2025-06-11 이면,
    # macros.ds_add(ds, -2) → "2025-06-09"
    run_date = "{{ macros.ds_add(ds, -2) }}"
    # ───────────────────────────────────────────────────────

    # ① 로그 태스크
    log_params = log_inputs(run_date=run_date)

    # ② spark-submit arguments
    arguments = [
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", dag_name,
    ]
    for k, v in spark_configs.items():
        arguments += ["--conf", f"{k}={v}"]

    # UI proxy 설정
    arguments += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ]

    # 애플리케이션 + 2일 전 날짜 인자
    arguments += [
        "--py-files", "s3a://creatz-airflow-jobs/raw_to_parquet/zips/dependencies.zip",
        "s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",
        "--start-date", run_date,
        "--end-date",   run_date,
    ]

    spark_submit = KubernetesPodOperator(
        task_id="run_raw_to_parquet_daily",
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
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits=  {"memory": "2Gi",   "cpu": "1000m"},
        ),
    )

    # ③ 워크플로우 연결
    log_params >> spark_submit

# DAG 인스턴스 생성
dag = raw_to_parquet_dag()