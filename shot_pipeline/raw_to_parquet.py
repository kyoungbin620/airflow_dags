from airflow.decorators import dag
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

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": Param(default="2025-05-01", type="string", format="%Y-%m-%d", description="시작 날짜"),
        "end_date":   Param(default="2025-05-02", type="string", format="%Y-%m-%d", description="종료 날짜"),
        "hour":       Param(default=None, type=["null", "string"], description="실행할 시간 (00~23). 입력하지 않으면 00시부터 23시까지 전체 반복 실행"),
    },
    tags=["spark", "s3", "parquet"],
)


def raw_to_parquet_dag():


    @task()
    def log_inputs(params=None):
        import logging
        logging.info(f"[INPUT] start_date: {params['start_date']}")
        logging.info(f"[INPUT] end_date: {params['end_date']}")
        logging.info(f"[INPUT] hour: {params.get('hour')}")
    
    log_params = log_inputs()

    arguments = [
        # Kubernetes 클러스터 모드 접속
        "--master", api_server,
        "--deploy-mode", "cluster",

        # Spark-on-K8s 기본 설정
        "--name", dag_name,
        "--conf", "spark.kubernetes.namespace=airflow",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=airflow-irsa",
        "--conf", "spark.kubernetes.container.image.pullSecrets=ecr-pull-secret",
        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

        # 리소스 설정
        "--conf", "spark.executor.instances=1",
        "--conf", "spark.executor.memory=512m",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
        "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",

        

        # 이미지 설정
        "--conf", f"spark.kubernetes.container.image={spark_image}",
        "--conf", f"spark.kubernetes.driver.container.image={spark_image}",

        # UI 프록시 라우팅
        "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
        "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",

        # (★ 필수) S3→로컬 스테이징 경로 지정
        "--conf", "spark.kubernetes.file.upload.path=local:///opt/spark/tmp",

        # Python 의존성(zip) 및 애플리케이션 리소스
        "--py-files", "s3a://creatz-airflow-jobs/raw_to_parquet/zips/dependencies.zip",
        "s3a://creatz-airflow-jobs/raw_to_parquet/scripts/run_raw_to_parquet.py",

        # 사용자 파라미터
        "--start-date", "{{ params.start_date }}",
        "--end-date",   "{{ params.end_date }}",
        "--hour",       "{{ params.hour or '' }}",
    ]

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
            requests={"memory": "1Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        ),
    )

    log_params >> spark_submit

dag_instance = raw_to_parquet_dag()
