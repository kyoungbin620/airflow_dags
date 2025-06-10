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
    # 매일 UTC 02:00에 실행
    schedule_interval="0 2 * * *",
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

    # 기존의 log_inputs() 호출을 아래 한 줄로 바꿉니다.
    log_params = log_inputs(run_date="{{ ds }}")

    # spark-submit 기본 인자
    arguments = [
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", dag_name,
    ]

    # spark_configs 를 --conf 형식으로 추가
    for key, value in spark_configs.items():
        arguments += ["--conf", f"{key}={value}"]

    # Spark UI 프록시 설정
    arguments += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ]

    # 파이썬 애플리케이션 경로와 날짜 매크로
    arguments += [
        "--py-files", "s3a://creatz-airflow-jobs/raw_to_parquet/zips/dependencies.zip",
        "s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",
        # Airflow 매크로 ds 는 “실행 기준일 전날”을 가리킵니다
        "--start-date", "{{ ds }}",
        "--end-date",   "{{ ds }}",
    ]

    # KubernetesPodOperator 정의
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

    # 로그 로깅 후 Spark 제출
    log_params >> spark_submit

# DAG 인스턴스 생성
dag_instance = raw_to_parquet_dag()
