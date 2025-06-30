from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import timedelta
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

# ── Dag 기본 설정 ──
dag_name    = "spark_shot_summary_range"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server  = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ── Spark 공통 설정 (외부에서 가져온 느낌으로) ──
spark_configs = {
    "spark.driver.memory":                        "1g",
    "spark.driver.maxResultSize":                 "512m",
    "spark.executor.memory":                      "1g",
    "spark.executor.memoryOverhead":              "512m",
    "spark.dynamicAllocation.enabled":            "true",
    "spark.dynamicAllocation.minExecutors":       "1",
    "spark.dynamicAllocation.maxExecutors":       "1",
    "spark.dynamicAllocation.initialExecutors":   "1",
    "spark.executor.cores":                       "1",
    "spark.sql.adaptive.enabled":                 "true",
    "spark.sql.adaptive.coalescePartitions.enabled":"true",
    "spark.sql.shuffle.partitions":               "20",
    "spark.memory.offHeap.enabled":               "true",
    "spark.memory.offHeap.size":                  "512m",
    "spark.sql.files.maxPartitionBytes":          "134217728",
    "spark.default.parallelism":                  "4",
    "spark.sql.broadcastTimeout":                 "600",
    "spark.network.timeout":                      "800",
    "spark.sql.sources.partitionOverwriteMode":   "dynamic",
    "spark.hadoop.fs.s3a.endpoint":               "s3.us-west-2.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region":        "us-west-2",
    "spark.hadoop.fs.s3a.access.style":           "PathStyle",
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3.impl":                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "spark.kubernetes.namespace":                 "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName":"airflow-irsa",
    "spark.kubernetes.container.image.pullSecrets": "ecr-pull-secret",
    "spark.kubernetes.container.image":           spark_image,
    "spark.kubernetes.driver.container.image":    spark_image,
    "spark.kubernetes.file.upload.path":          "local:///opt/spark/tmp",
    "spark.kubernetes.driver.request.cores":      "1",
    "spark.kubernetes.driver.limit.cores":        "2",
    "spark.kubernetes.executor.request.cores":    "1",
    "spark.kubernetes.executor.limit.cores":      "2",
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(
            default="2025-06-01",
            type="string",
            format="%Y-%m-%d",
            description="처리 시작 날짜"
        ),
        "end_date": Param(
            default="2025-06-03",
            type="string",
            format="%Y-%m-%d",
            description="처리 종료 날짜"
        ),
    },
    tags=["spark", "jdbc", "s3"],
)
def spark_shot_summary_range_dag():

    @task()
    def log_inputs(params=None):
        import logging
        logging.info(f"[INPUT] start_date = {params['start_date']}")
        logging.info(f"[INPUT]   end_date = {params['end_date']}")

    log_task = log_inputs()

    # 공통 --conf 리스트 생성
    common_conf = []
    for k, v in spark_configs.items():
        common_conf += ["--conf", f"{k}={v}"]

    # UI Proxy 설정 (선택)
    common_conf += [
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
    ]

    spark_task = KubernetesPodOperator(
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
        get_logs=True,
        is_delete_operator_pod=False,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "500m"},
            limits=  {"memory": "4Gi", "cpu": "1000m"},
        ),
    )

    log_task >> spark_task

spark_shot_summary_range_dag_instance = spark_shot_summary_range_dag()
