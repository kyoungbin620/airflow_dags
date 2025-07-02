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

# ────────────────────────────────────────────
# Spark 설정 중앙 관리
# ────────────────────────────────────────────
spark_configs = {
    # Driver
    "spark.driver.cores": "2",
    "spark.driver.memory": "6g",
    "spark.driver.memoryOverhead": "512m",
    "spark.driver.maxResultSize": "1g",

    # Executor
    "spark.executor.cores": "2",
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.initialExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "4",

    # Kubernetes 리소스
    "spark.kubernetes.driver.request.cores": "2",
    "spark.kubernetes.driver.limit.cores":   "2",
    "spark.kubernetes.executor.request.cores": "2",
    "spark.kubernetes.executor.limit.cores":   "2",

    # AQE 및 파일 크기 조정
    "spark.sql.adaptive.enabled":                         "true",
    "spark.sql.adaptive.coalescePartitions.enabled":      "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":    str(10 * 1024 * 1024),  # 10MB
    "spark.sql.shuffle.partitions":                       "128",
    "spark.default.parallelism":                          "128",

    # 파티션 덮어쓰기
    "spark.sql.sources.partitionOverwriteMode":           "dynamic",

    # S3A 설정 (IRSA)
    "spark.hadoop.fs.s3a.endpoint":                       
        "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region":               
        "us-west-2",
    "spark.hadoop.fs.s3a.access.style":                   
        "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access":              
        "true",
    "spark.hadoop.fs.s3.impl":                            
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":      
        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

    # Hadoop FileOutputCommitter v1 사용
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "1",

    # Kubernetes 설정
    "spark.kubernetes.namespace":                         
        "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": 
        "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets":       
        "ecr-pull-secret",
    "spark.kubernetes.container.image":                   
        spark_image,
    "spark.kubernetes.driver.container.image":            
        spark_image,
    "spark.kubernetes.file.upload.path":                  
        "local:///opt/spark/tmp",

    # Spark UI proxy
    "spark.kubernetes.driver.label.spark-ui-selector":    
        dag_id,
}

@dag(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param("2025-05-01", type="string", format="%Y-%m-%d"),
        "end_date":   Param("2025-05-02", type="string", format="%Y-%m-%d"),
    },
)
def log_to_parquet_dag():

    # spark_configs → --conf 인자 리스트 변환
    spark_conf_args = []
    for k, v in spark_configs.items():
        spark_conf_args += ["--conf", f"{k}={v}"]

    # UI proxyBase 추가
    spark_conf_args += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_id}",
        "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
    ]

    spark_submit = KubernetesPodOperator(
        task_id="run_spark_submit_s3_script",
        name="spark-submit-s3-script",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", api_server,
            "--deploy-mode", "cluster",
            "--name",       dag_id,
            *spark_conf_args,
            # 실제 처리 스크립트 위치
            "s3a://creatz-airflow-jobs/monitoring/scripts/monitoring_logs_to_parquet_daily.py",
            "--start-date", "{{ params.start_date }}",
            "--end-date",   "{{ params.end_date }}",
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference("ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits=  {"cpu": "1000m","memory": "2Gi"},
        ),
        node_selector={"intent": "spark"},
    )

    spark_submit

dag = log_to_parquet_dag()
