from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
# ─────────────────────────────
# 공통 설정
# ─────────────────────────────
dag_name    = "raw-to-swingdata-daily"
api_server  = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

# ─────────────────────────────
# Spark 공통 sparkConf 설정
# ─────────────────────────────

# Spark 공통 구성
spark_configs = {
    "spark.dynamicAllocation.enabled": "true",              # Executor 수 자동 조정 활성화
    "spark.dynamicAllocation.minExecutors": "2",            # 최소 Executor 수
    "spark.dynamicAllocation.initialExecutors": "4",        # 초기 Executor 수
    "spark.dynamicAllocation.maxExecutors": "32",           # 최대 Executor 수 (Karpenter가 자동으로 노드 증설)
    "spark.dynamicAllocation.executorIdleTimeout": "600s", # 10분 (600초)으로 설정 권장

    # ─────────────────────────────
    # 리소스 요청/제한 (Kubernetes 스케줄링용)
    # ─────────────────────────────
    "spark.kubernetes.driver.request.cores": "2",           # 드라이버가 요청하는 CPU
    "spark.kubernetes.driver.limit.cores": "2",             # 드라이버 최대 사용 CPU
    "spark.kubernetes.executor.request.cores": "2",         # Executor가 요청하는 CPU
    "spark.kubernetes.executor.limit.cores": "2",           # Executor 최대 사용 CPU

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

    "spark.speculation": "false",

    # ─────────────────────────────
    # Kubernetes 설정
    # ─────────────────────────────
    "spark.kubernetes.namespace": "airflow",                          # 실행 namespace
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",         # ECR 인증용 secret

    # ─────────────────────────────
    # 노드 선택 및 배치 제어
    # ─────────────────────────────
    "spark.kubernetes.executor.node.selector.intent": "spark",       # 노드 선택자 (NodePool과 연결)

        # 이벤트 로그
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://aim-spark/spark-events"
}


@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="0 1 * * *",  # 매일 UTC 1시에 실행
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "s3", "parquet"],
)
def raw_to_swingdata_daily_dag():

    @task()
    def log_date(yesterday: str):
        import logging
        logging.info(f"[INPUT] processing date: {yesterday}")

    date_template = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
    log_task = log_date(yesterday=date_template)

    # ─────────────────────────────
    # 1) Raw → Parquet
    # ─────────────────────────────
    raw_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind":       "SparkApplication",
        "metadata": {
            "name":      f"{dag_name}-raw",
            "namespace": "airflow",
        },
        "spec": {
            "type":                "Python",
            "mode":                "cluster",
            "sparkVersion":        "3.5.3",
            "image":               "577638362884.dkr.ecr.us-west-2.amazonaws.com/spark-job/shot-pipeline:raw-parquet-1.0.0",
            "imagePullPolicy":     "Always",
            "mainApplicationFile": "local:///home/spark/jobs/scripts/run_raw_to_parquet.py",
            "deps":{
                "pyFiles": ["local:///home/spark/jobs/scripts/dependencies/"]
                },
            "arguments": [
                "--start-date", date_template,
                "--end-date",   date_template,
            ],
            "sparkConf":          spark_configs,
            "driver": {
                "cores":          2,
                "memory":         "6g",
                "memoryOverhead": "512m",
                "serviceAccount": "airflow-irsa",
                "nodeSelector":   {"intent": "spark"},
                "labels": {
                    "component": "spark-driver"
                },
                "podAnnotations": {"karpenter.sh/do-not-disrupt": "true"} 
            },

            "executor": {
                "cores":          2,
                "memory":         "2g",
                "memoryOverhead": "512m",
                "nodeSelector":   {"intent": "spark"},
                "labels": {
                    "component": "spark-executor"
                },
            },

            "restartPolicy": {"type": "Never"},
        }
    }

    raw_spark = SparkKubernetesOperator(
        task_id="run_raw_to_base_daily",            # Airflow 내에서 이 Operator를 식별할 ID
        name="raw-to-base-pipeline",                # 생성될 SparkApplication CRD의 metadata.name
        namespace="airflow",                        # SparkApplication이 생성될 k8s 네임스페이스
        template_spec=raw_app,                      # 위에서 정의한 SparkApplication 스펙 전체
        get_logs=True,                              # 드라이버 로그를 Airflow UI에서 실시간으로 가져올지 여부
        do_xcom_push=False,                         # SparkApplication 결과를 XCom에 저장할지 여부
        delete_on_termination=False,                 # 완료 후 k8s 리소리(Driver/Executor Pod 및 CRD)를 자동 삭제
        startup_timeout_seconds=600,                # SparkApplication이 준비 상태를 기다리는 최대 시간(초)
        log_events_on_failure=True,                 # 실패 시 k8s 이벤트를 함께 로깅해서 문제 원인 파악을 돕기
        reattach_on_restart=True,                   # Airflow 스케줄러 재시작 후에도 기존 SparkApplication에 재연결
        kubernetes_conn_id="kubernetes_default",    # Airflow에 설정된 k8s 클러스터 연결 ID
    )

    # ─────────────────────────────
    # 2) Parquet → SwingData
    # ─────────────────────────────
    base_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind":       "SparkApplication",
        "metadata": {
            "name":      f"{dag_name}-base",
            "namespace": "airflow",
        },
        "spec": {
            "type":                "Python",
            "mode":                "cluster",
            "sparkVersion":        "3.5.3",
            "image":               "577638362884.dkr.ecr.us-west-2.amazonaws.com/spark-job/shot-pipeline:refine-parquet-1.0.0",
            "imagePullPolicy":     "Always",
            "mainApplicationFile": "local:///home/spark/jobs/scripts/run_swingdata_extract_pipeline.py",
            "arguments": [
                "--start-date", date_template,
                "--end-date",   date_template,
            ],
            "sparkConf":          spark_configs,
            "driver": {
                "cores":          2,
                "memory":         "6g",
                "memoryOverhead": "512m",
                "serviceAccount": "airflow-irsa",
                "nodeSelector":   {"intent": "spark"},
                "labels": {
                    "component": "spark-driver"
                },
                "podAnnotations": {"karpenter.sh/do-not-disrupt": "true"} 
            },

            "executor": {
                "cores":          2,
                "memory":         "2g",
                "memoryOverhead": "512m",
                "nodeSelector":   {"intent": "spark"},
                "labels": {
                    "component": "spark-executor"
                },
            },

            "restartPolicy": {"type": "Never"},
        }
    }


    base_spark = SparkKubernetesOperator(
        task_id="run_base_to_swingdata_daily",
        name="base-to-swingdata-pipeline",
        namespace="airflow",
        template_spec=base_app,
        get_logs=True,
        do_xcom_push=False,
        delete_on_termination=True,
        startup_timeout_seconds=600,
        log_events_on_failure=True,
        reattach_on_restart=True,
        kubernetes_conn_id="kubernetes_default",
    )

    # ─────────────────────────────
    # 3) SwingData → Database
    # ─────────────────────────────
    db_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind":       "SparkApplication",
        "metadata": {
            "name":      f"{dag_name}-db",
            "namespace": "airflow",
        },
        "spec": {
            "type":                "Python",
            "mode":                "cluster",
            "sparkVersion":        "3.5.3",
            "image":               "577638362884.dkr.ecr.us-west-2.amazonaws.com/spark-job/shot-pipeline:feature-db-1.0.0",
            "imagePullPolicy":     "Always",
            "mainApplicationFile": "local:///home/spark/jobs/scripts/run_swingdata_extract_database.py",
            "deps":{
                "jars": [
                    "local:///opt/spark/jars/postgresql-42.7.3.jar",
                    ]
                },
            "arguments": [
                "--start-date",    date_template,
                "--end-date",      date_template,
                "--input_s3_base", "s3a://creatz-aim-swing-mx-data-prod/parquet/shotinfo_swingtrace",
                "--jdbc_url",      "jdbc:postgresql://10.133.135.243:5432/monitoring",
                "--jdbc_table",    "shot_summary",
                "--jdbc_user",     "aim",
                "--jdbc_password", "aim3062",
            ],
            "sparkConf":          spark_configs,
            "driver": {
                "cores":          2,
                "memory":         "6g",
                "memoryOverhead": "512m",
                "serviceAccount": "airflow-irsa",
                "nodeSelector":   {"intent": "spark"},
                "labels": {
                    "component": "spark-driver"
                },
                "podAnnotations": {"karpenter.sh/do-not-disrupt": "true"}
            },
            "executor": {
                "cores":          2,
                "memory":         "4g",
                "memoryOverhead": "1g",
                "nodeSelector":   {"intent": "spark"},
                "labels": {
                    "component": "spark-executor"
                },
            },
            "restartPolicy": {"type": "Never"},
        }
    }


    db_spark = SparkKubernetesOperator(
        task_id="run_spark_shot_summary_daily",
        name="spark-shot-summary-daily",
        namespace="airflow",
        template_spec=db_app,
        get_logs=True,
        do_xcom_push=False,
        delete_on_termination=True,
        startup_timeout_seconds=600,
        log_events_on_failure=True,
        reattach_on_restart=True,
        kubernetes_conn_id="kubernetes_default",
    )

    # ─────────────────────────────
    # 태스크 의존성 정의
    # ─────────────────────────────
    log_task >> raw_spark >> base_spark >> db_spark

# DAG 등록
raw_to_swingdata_daily = raw_to_swingdata_daily_dag()
