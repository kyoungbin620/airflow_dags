import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

# -----------------------------------------------------------------------------
# DAG 설정
# -----------------------------------------------------------------------------
DAG_NAME = "raw_to_combined_swingdata_daily_auto"  # DAG 식별자
SPARK_IMAGE = (
    "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
)  # 사용할 Spark 이미지
API_SERVER = (
    "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"
)  # Kubernetes API 서버 주소

# 기본 태스크 인자
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Spark 공통 설정 (설명 한글)
SPARK_CONFIGS = {
    # 드라이버 메모리 설정
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",

    # Executor 메모리 및 오버헤드 설정
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",

    # 동적 할당 및 Executor 개수 제한
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "1",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.executor.cores": "1",  # Executor당 코어 수

    # Spark SQL 성능 최적화
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

    # S3 연결 설정
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
    "spark.hadoop.fs.s3a.access.style": "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    ),

    # Kubernetes 설정
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.kubernetes.container.image": SPARK_IMAGE,
    "spark.kubernetes.driver.container.image": SPARK_IMAGE,
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",

    # 리소스 요청/제한
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores": "2",
    "spark.kubernetes.executor.request.cores": "1",
    "spark.kubernetes.executor.limit.cores": "2",
}


def build_spark_args(
    name_suffix: str,
    script_path: str,
    start_date: str,
    end_date: str,
) -> list:
    """
    Spark-submit 인자를 생성하는 헬퍼 함수

    :param name_suffix: UI 프록시 및 라벨에 적용할 접미사
    :param script_path: 실행할 Python 스크립트 경로
    :param start_date: 파이프라인 시작 날짜
    :param end_date: 파이프라인 종료 날짜
    :return: spark-submit 인자 리스트
    """
    args = [
        "--master",
        API_SERVER,
        "--deploy-mode",
        "cluster",
        "--name",
        f"{DAG_NAME}-{name_suffix}",
    ]

    # Spark 설정 추가
    for key, val in SPARK_CONFIGS.items():
        args += ["--conf", f"{key}={val}"]

    # UI 프록시 및 드라이버 라벨 설정
    args += [
        "--conf",
        f"spark.ui.proxyBase=/spark-ui/{DAG_NAME}-{name_suffix}",
        "--conf",
        f"spark.kubernetes.driver.label.spark-ui-selector={DAG_NAME}-{name_suffix}",
    ]

    # 스크립트와 파라미터 추가
    args += [
        script_path,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]

    return args


@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
        schedule_interval="0 17 * * *",  # 매일 02:00 KST (17:00 UTC 전날)에 실행
