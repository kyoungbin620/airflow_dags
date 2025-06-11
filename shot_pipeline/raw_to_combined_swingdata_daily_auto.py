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
    schedule_interval="0 17 * * *"  # 매일 02:00 KST (17:00 UTC 전날)에 실행,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(
            default="2025-05-01", type="string", format="%Y-%m-%d"
        ),
        "end_date": Param(
            default="2025-05-02", type="string", format="%Y-%m-%d"
        ),
    },
    tags=["spark", "parquet", "s3"],
)
def combined_spark_pipeline():
    """
    Airflow DAG 정의:
    1단계: raw 데이터를 base Parquet으로 변환
    2단계: shotinfo 및 swingtrace JSON 필드 추출
    """

    @task()
    def log_parameters(start_date: str, end_date: str):
        """
        시작 및 종료 날짜 파라미터 로깅
        """
        logging.info(f"시작 날짜: {start_date}")
        logging.info(f"종료 날짜: {end_date}")

    # 파라미터 로깅 태스크
    log_task = log_parameters(
        "{{ params.start_date }}", "{{ params.end_date }}"
    )

    # Step 1: Raw -> Base Parquet 변환
    base_spark_args = build_spark_args(
        name_suffix="base",
        script_path="s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",
        start_date="{{ macros.ds_add(ds, -1) }}", # 실행 시점 기준 전날(실제 처리일 기준 이틀 전),
        end_date="{{ macros.ds_add(ds, -1) }}",
    )

    raw_to_base = KubernetesPodOperator(
        task_id="run_raw_to_parquet",
        name="raw-to-parquet",
        namespace="airflow",
        image=SPARK_IMAGE,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=base_spark_args,
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    # Step 2: Base -> 통합 JSON 추출
    combined_spark_args = build_spark_args(
        name_suffix="combined",
        script_path="s3a://creatz-airflow-jobs/monitoring/scripts/run_combined_json_extract.py",
        start_date="{{ macros.ds_add(ds, -1) }}",
        end_date="{{ macros.ds_add(ds, -1) }}",
    )

    extract_to_combined = KubernetesPodOperator(
        task_id="run_combined_extract",
        name="parquet-to-combined",
        namespace="airflow",
        image=SPARK_IMAGE,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=combined_spark_args,
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    # 태스크 의존성 정의
    log_task >> raw_to_base >> extract_to_combined


dag_instance = combined_spark_pipeline()
