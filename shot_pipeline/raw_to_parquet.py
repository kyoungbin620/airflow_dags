from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

dag_name = "run_raw_to_parquet_hour"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Spark 설정 중앙 관리
spark_configs = {
    # 메모리 관리 (노드 상황 고려 - 기존 설정 유지)
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "512m",
    
    # Executor 설정 (executor 수는 유지하고 코어 수만 증가)
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
    
    # 동적 파티션 관리
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    
    # S3 Endpoint 설정
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
    "spark.hadoop.fs.s3a.access.style": "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    
    # Kubernetes 설정
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "spark.kubernetes.container.image": spark_image,
    "spark.kubernetes.driver.container.image": spark_image,
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",
    
    # 리소스 요청/제한 설정 (CPU만 증가)
    "spark.kubernetes.driver.request.cores": "1",
    "spark.kubernetes.driver.limit.cores": "2",
    "spark.kubernetes.executor.request.cores": "2",
    "spark.kubernetes.executor.limit.cores": "4",
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


def raw_to_parquet_dag():


    @task()
    def log_inputs(params=None):
        import logging
        logging.info(f"[INPUT] start_date: {params['start_date']}")
        logging.info(f"[INPUT] end_date: {params['end_date']}")
    
    log_params = log_inputs()

    # Spark 설정을 arguments로 변환
    arguments = [
        # Kubernetes 클러스터 모드 접속
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", dag_name,
    ]
    
    # 설정을 arguments에 추가
    for key, value in spark_configs.items():
        arguments.extend(["--conf", f"{key}={value}"])
    
    # Spark 로그 레벨 설정 추가
    # arguments.extend([
    #     # Driver와 Executor 모두 동일한 로그 레벨 적용
    #     "--conf", "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=INFO,console -Dlog4j.appender.console=org.apache.log4j.ConsoleAppender -Dlog4j.appender.console.layout=org.apache.log4j.PatternLayout \"-Dlog4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %p %c{1}: %m%n\"",
    #     "--conf", "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=INFO,console -Dlog4j.appender.console=org.apache.log4j.ConsoleAppender -Dlog4j.appender.console.layout=org.apache.log4j.PatternLayout \"-Dlog4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %p %c{1}: %m%n\"",
    #     # Spark 자체 로그 레벨 설정
    #     "--conf", "spark.log.level=INFO"
    # ])
    
    # UI 프록시 라우팅 (동적 설정이라 별도 추가)
    arguments.extend([
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ])
    
    # 애플리케이션 리소스
    arguments.extend([
        "--py-files", "s3a://creatz-airflow-jobs/raw_to_parquet/zips/dependencies.zip",
        "s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",
        "--start-date", "{{ params.start_date }}",
        "--end-date", "{{ params.end_date }}",
    ])

    spark_submit = KubernetesPodOperator(
        task_id="run_raw_to_parquet_hour",
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
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    log_params >> spark_submit

dag_instance = raw_to_parquet_dag()
