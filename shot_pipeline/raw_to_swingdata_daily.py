from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# 공통 설정
dag_name = "raw_to_swingdata_daily"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"

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
    "spark.executor.cores": "2",
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.initialExecutors": "4",
    "spark.dynamicAllocation.maxExecutors": "32",

    # ─────────────────────────────
    # 리소스 요청/제한 (Kubernetes 스케줄링용)
    # ─────────────────────────────
    "spark.kubernetes.driver.request.cores": "2",
    "spark.kubernetes.driver.limit.cores": "2",
    "spark.kubernetes.executor.request.cores": "2",
    "spark.kubernetes.executor.limit.cores": "2",

    # ─────────────────────────────
    # 쿼리 성능 최적화
    # ─────────────────────────────
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "128",
    "spark.default.parallelism": "128",
    "spark.sql.files.maxPartitionBytes": "134217728",

    # ─────────────────────────────
    # 네트워크/메모리 안정성
    # ─────────────────────────────
    "spark.network.timeout": "21600s",
    "spark.sql.broadcastTimeout": "18000s",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",

    # ─────────────────────────────
    # 데이터 저장 설정
    # ─────────────────────────────
    "spark.sql.sources.partitionOverwriteMode": "dynamic",

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
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.kubernetes.container.image": spark_image,
    "spark.kubernetes.driver.container.image": spark_image,
    "spark.kubernetes.file.upload.path": "local:///opt/spark/tmp",

    # ─────────────────────────────
    # 노드 선택 및 배치 제어
    # ─────────────────────────────
    "spark.kubernetes.executor.node.selector.intent": "spark",

    # 이벤트 로그
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://aim-spark/spark-events"
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="0 1 * * *",
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

    # Raw -> Base SparkKubernetesOperator 사용
    raw_task = SparkKubernetesOperator(
        task_id="run_raw_to_base_daily",
        namespace="airflow",
        application_file="raw_spark_app.yaml",  # 별도 YAML 파일 필요
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        # SparkApplication 정의
        spark_app_name=f"{dag_name}-raw-{{{{ ts_nodash }}}}",
        spark_config={
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": spark_image,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",
            "arguments": [
                "--start-date", date_template,
                "--end-date", date_template,
            ],
            "sparkConf": spark_configs,
            "driver": {
                "cores": 2,
                "memory": "6g",
                "serviceAccount": "airflow-irsa",
                "nodeSelector": {"intent": "spark"},
            },
            "executor": {
                "cores": 2,
                "memory": "2g",
                "instances": 4,
                "serviceAccount": "airflow-irsa",
                "nodeSelector": {"intent": "spark"},
            },
        },
    )

    # Base -> SwingData SparkKubernetesOperator 사용
    base_task = SparkKubernetesOperator(
        task_id="run_base_to_swingdata_daily",
        namespace="airflow",
        application_file="base_spark_app.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        spark_app_name=f"{dag_name}-base-{{{{ ts_nodash }}}}",
        spark_config={
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": spark_image,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "s3a://creatz-airflow-jobs/base_to_swingdata/scripts/run_swingdata_extract_pipeline.py",
            "arguments": [
                "--start-date", date_template,
                "--end-date", date_template,
            ],
            "sparkConf": spark_configs,
            "driver": {
                "cores": 2,
                "memory": "6g",
                "serviceAccount": "airflow-irsa",
                "nodeSelector": {"intent": "spark"},
            },
            "executor": {
                "cores": 2,
                "memory": "2g",
                "instances": 4,
                "serviceAccount": "airflow-irsa",
                "nodeSelector": {"intent": "spark"},
            },
        },
    )

    # 기존 insert_db_task는 그대로 유지 (문제없이 동작한다면)
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
    from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

    # 공통 --conf 리스트
    common_conf = []
    for key, value in spark_configs.items():
        common_conf += ["--conf", f"{key}={value}"]
    common_conf += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ]

    insert_db_task = KubernetesPodOperator(
        task_id="run_spark_shot_summary_daily",
        name="spark-shot-summary-daily",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com",
            "--deploy-mode", "cluster",
            "--name", f"{dag_name}-job",
            *common_conf,
            "--jars", "s3a://creatz-airflow-jobs/swingdata_to_database/jars/postgresql-42.7.3.jar",
            "s3a://creatz-airflow-jobs/swingdata_to_database/scripts/run_swingdata_extract_database.py",
            "--start_date", date_template,
            "--end_date", date_template,
            "--input_s3_base", "s3a://creatz-aim-swing-mx-data-prod/parquet/shotinfo_swingtrace",
            "--jdbc_url", "jdbc:postgresql://10.133.135.243:5432/monitoring",
            "--jdbc_table", "shot_summary",
            "--jdbc_user", "aim",
            "--jdbc_password", "aim3062",
        ],
        get_logs=True,
        is_delete_operator_pod=False,
        node_selector={"intent": "spark"},
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "500m"},
            limits={"memory": "4Gi", "cpu": "1000m"},
        ),
    )

    log_task >> raw_task >> base_task >> insert_db_task

raw_to_swingdata_daily_dag_instance = raw_to_swingdata_daily_dag()