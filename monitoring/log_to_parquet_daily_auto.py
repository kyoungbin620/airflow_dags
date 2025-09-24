from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_name    = "log_to_parquet_daily_auto"
api_server  = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}



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



    # ─────────────────────────────
    # Kubernetes 설정
    # ─────────────────────────────
    "spark.kubernetes.namespace": "airflow",                          # 실행 namespace
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",         # ECR 인증용 secret

    # ─────────────────────────────
    # 노드 선택 및 배치 제어
    # ─────────────────────────────
    "spark.kubernetes.driver.node.selector.intent": "spark-driver",
    "spark.kubernetes.driver.node.selector.workload": "spark-airflow",
    "spark.kubernetes.executor.node.selector.intent":"spark-executor",
    "spark.kubernetes.executor.node.selector.workload":"spark-airflow",

        # 이벤트 로그
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://aim-spark/spark-events"
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="30 0 * * *",    # <-- 변경된 스케줄: 매일 UTC 0시 30분에 실행
    start_date=days_ago(1),           # DAG 최초 실행 기준
    catchup=False,
    tags=["spark", "s3", "parquet"],
)
def log_to_parquet_daily_dag():
    date_template = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

    logs_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind":       "SparkApplication",
        "metadata": {
            "name":      f"{dag_name}",
            "namespace": "airflow",
        },
        "spec": {
            "type":                "Python",
            "mode":                "cluster",
            "sparkVersion":        "3.5.3",
            "image":               "577638362884.dkr.ecr.us-west-2.amazonaws.com/spark-job/logs-monitoring:raw-parquet-1.0.0",
            "imagePullPolicy":     "Always",
            "mainApplicationFile": "local:///home/spark/jobs/scripts/monitoring_logs_to_parquet_daily.py",
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
                "nodeSelector": {"intent": "spark-driver", "workload": "spark-airflow"},
                "labels": {
                    "component": "spark-driver"
                },
            },

            "executor": {
                "cores":          2,
                "memory":         "2g",
                "memoryOverhead": "512m",
                "nodeSelector": {"intent": "spark-executor", "workload": "spark-airflow"},
                "labels": {
                    "component": "spark-executor"
                },
            },

            "restartPolicy": {"type": "Never"},
        }
    }


    logs_spark = SparkKubernetesOperator(
        task_id="monitoring-log-task-daily",
        name="logs-to-parquet-daily",
        namespace="airflow",
        template_spec=logs_app,
        get_logs=True,
        do_xcom_push=False,
        delete_on_termination=True,
        startup_timeout_seconds=600,
        log_events_on_failure=True,
        reattach_on_restart=True,
        kubernetes_conn_id="kubernetes_default",
    )
    logs_app

dag = log_to_parquet_daily_dag()  # ← 반드시 global scope에 있어야 함