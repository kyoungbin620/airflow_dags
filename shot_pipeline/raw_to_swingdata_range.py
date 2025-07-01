from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

# 공통 설정
dag_name = "raw_to_swingdata_range"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

# Spark 공통 구성
spark_configs = {
    # ─────────────────────────────
    # 드라이버 설정
    # ─────────────────────────────
    "spark.driver.cores": "2",
    "spark.driver.memory": "6g",
    "spark.driver.memoryOverhead": "512m",
    "spark.driver.maxResultSize": "1g",

    # ─────────────────────────────
    # 실행자 (Executor) 설정
    # ─────────────────────────────
    "spark.executor.cores": "2",                            # Executor 하나당 사용할 CPU 수
    "spark.executor.memory": "2g",                          # Executor 메모리
    "spark.executor.memoryOverhead": "512m",                  # JVM 외 메모리 오버헤드 (압축 해제시 필요)

    
    "spark.dynamicAllocation.enabled": "true",              # Executor 수 자동 조정 활성화
    "spark.dynamicAllocation.minExecutors": "2",            # 최소 Executor 수
    "spark.dynamicAllocation.initialExecutors": "4",        # 초기 Executor 수
    "spark.dynamicAllocation.maxExecutors": "32",           # 최대 Executor 수 (Karpenter가 자동으로 노드 증설)

    # ─────────────────────────────
    # 리소스 요청/제한 (Kubernetes 스케줄링용)
    # ─────────────────────────────
    "spark.kubernetes.driver.request.cores": "1",           # 드라이버가 요청하는 CPU
    "spark.kubernetes.driver.limit.cores": "1",             # 드라이버 최대 사용 CPU
    "spark.kubernetes.executor.request.cores": "1",         # Executor가 요청하는 CPU
    "spark.kubernetes.executor.limit.cores": "1",           # Executor 최대 사용 CPU

    # ─────────────────────────────
    # 쿼리 성능 최적화
    # ─────────────────────────────
    "spark.sql.adaptive.enabled": "true",                   # AQE 활성화
    "spark.sql.adaptive.coalescePartitions.enabled": "true",# 작은 파티션 자동 병합
    "spark.sql.shuffle.partitions": "128",                   # 셔플 파티션 수 (적절한 병렬성 확보)
    "spark.default.parallelism": "128",                      # 기본 병렬 작업 수
    "spark.sql.files.maxPartitionBytes": "134217728",       # 파일 파티션 크기 (128MB)
    # ─────────────────────────────
    # 네트워크/메모리 안정성
    # ─────────────────────────────
    "spark.network.timeout": "21600s",                         # 작업 타임아웃
    "spark.sql.broadcastTimeout": "18000s",                    # 브로드캐스트 조인 타임아웃
    "spark.memory.offHeap.enabled": "true",                 # 오프힙 메모리 활성화
    "spark.memory.offHeap.size": "512m",                    # 오프힙 메모리 크기

    # ─────────────────────────────
    # 데이터 저장 설정
    # ─────────────────────────────
    "spark.sql.sources.partitionOverwriteMode": "dynamic",  # Hive-style 파티션 덮어쓰기

    # ─────────────────────────────
    # S3 설정 (IRSA 기반 접근)
    # ─────────────────────────────
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
    "spark.hadoop.fs.s3a.access.style": "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

    # ─────────────────────────────
    # Kubernetes 설정
    # ─────────────────────────────
    "spark.kubernetes.namespace": "airflow",                          # 실행 namespace
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",  # IRSA 서비스계정
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",         # ECR 인증용 secret
    "spark.kubernetes.container.image": spark_image,                  # 기본 컨테이너 이미지
    "spark.kubernetes.driver.container.image": spark_image,          # 드라이버 이미지
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",   # 임시 파일 업로드 경로

    # ─────────────────────────────
    # 노드 선택 및 배치 제어
    # ─────────────────────────────
    "spark.kubernetes.executor.node.selector.intent": "spark",       # 노드 선택자 (NodePool과 연결)
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(default="2025-05-01", type="string", format="%Y-%m-%d", description="시작 날짜"),
        "end_date":   Param(default="2025-05-02", type="string", format="%Y-%m-%d", description="종료 날짜"),
    },
    tags=["spark", "s3", "parquet"],
)
def raw_to_swingdata_range_dag():
    @task()
    def log_inputs(params=None):
        import logging
        logging.info(f"[INPUT] start_date: {params['start_date']}")
        logging.info(f"[INPUT] end_date: {params['end_date']}")

    log_task = log_inputs()

    # 공통 arguments 변환
    common_conf = []
    for key, value in spark_configs.items():
        common_conf += ["--conf", f"{key}={value}"]
    # UI Proxy 설정
    common_conf += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ]

    # Raw -> Base SparkSubmit
    raw_args = [
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", f"{dag_name}-raw",
        *common_conf,
        "--py-files", "s3a://creatz-airflow-jobs/raw_to_parquet/zips/dependencies.zip",
        "s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",
        "--start-date", "{{ params.start_date }}",
        "--end-date", "{{ params.end_date }}",
    ]
    raw_task = KubernetesPodOperator(
        task_id="run_raw_to_base_range",
        name="raw-to-base-pipeline",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        node_selector={"intent": "spark"},
        do_xcom_push=False,
        arguments=raw_args,
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    # Base -> SwingData SparkSubmit
    base_args = [
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", f"{dag_name}-base",
        *common_conf,
        "s3a://creatz-airflow-jobs/base_to_swingdata/scripts/run_swingdata_extract_pipeline.py",
        "--start-date", "{{ params.start_date }}",
        "--end-date", "{{ params.end_date }}",
    ]
    base_task = KubernetesPodOperator(
        task_id="run_base_to_swingdata_range",
        name="base-to-swingdata-pipeline",
        namespace="airflow",
        image=spark_image,
        cmds=["sh", "-c"],
        arguments=[
            """
            echo '[WAIT] ConfigMap 생성 대기 중...';
            for i in $(seq 1 30); do
            if [ -f /opt/spark/conf/spark.properties ]; then echo '[OK] spark.properties 발견'; break; fi;
            echo '[WAIT] spark.properties 없음, 대기 중...';
            sleep 1;
            done;
            echo '[START] spark-submit 실행';
            /opt/spark/bin/spark-submit """ + " ".join(base_args)
        ],
        do_xcom_push=False,
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        node_selector={"intent": "spark"},
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )
    
    insert_db_task = KubernetesPodOperator(
        task_id="run_spark_shot_summary_range",
        name="spark-shot-summary-range",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            # 클러스터 및 모드
            "--master",      api_server,
            "--deploy-mode", "cluster",
            "--name",        f"{dag_name}-job",
            # 공통 conf
            *common_conf,
            # JAR 하나만 추가
            "--jars",       "s3a://creatz-airflow-jobs/swingdata_to_database/jars/postgresql-42.7.3.jar",
            # 스크립트 파일
            "s3a://creatz-airflow-jobs/swingdata_to_database/scripts/run_swingdata_extract_database.py",
            # 날짜 범위 파라미터
            "--start_date",  "{{ params.start_date }}",
            "--end_date",    "{{ params.end_date }}",
            "--input_s3_base", "s3a://creatz-aim-swing-mx-data-prod/parquet/shotinfo_swingtrace",
            "--jdbc_url",      "jdbc:postgresql://10.133.135.243:5432/monitoring",
            "--jdbc_table",    "shot_summary",
            "--jdbc_user",     "aim",
            "--jdbc_password", "aim3062",
        ],
        do_xcom_push=False,
        get_logs=True,
        is_delete_operator_pod=False,
        node_selector={"intent": "spark"},
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "500m"},
            limits=  {"memory": "4Gi", "cpu": "1000m"},
        ),
    )

    log_task >> raw_task >> base_task >> insert_db_task

# DAG 인스턴스화
raw_to_swingdata_range_dag_instance = raw_to_swingdata_range_dag()
