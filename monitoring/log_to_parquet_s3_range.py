from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_id = "log_to_parquet_s3_range"
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
    "spark.kubernetes.executor.node.selector.intent": "spark",       # 노드 선택자 (NodePool과 연결)

        # 이벤트 로그
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://aim-spark/spark-events"
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
    # ─────────────────────────────
    # 1) Parquet → SwingData
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
            "image":               "577638362884.dkr.ecr.us-west-2.amazonaws.com/spark-job/logs-monitoring:raw-parquet-1.0.0",
            "imagePullPolicy":     "Always",
            "mainApplicationFile": "local:///home/spark/jobs/scripts/monitoring_logs_to_parquet_daily.py",
            "arguments": [
                "--start-date", "{{ params.start_date }}",
                "--end-date", "{{ params.end_date }}",
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
        task_id="run_spark_submit_s3_script",
        name="spark-submit-s3-script",
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


    spark_submit

dag_instance = log_to_parquet_dag()
