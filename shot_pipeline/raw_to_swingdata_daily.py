from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import V1ResourceRequirements, V1LocalObjectReference

# 공통 설정
dag_name = "raw_to_swingdata_daily"
spark_image = "577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4"
api_server = "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

# Spark 공통 구성 (생략 – 기존 값 그대로)
spark_configs = {
    # … (생략)
}

@dag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="0 17 * * *",   # 매일 UTC 17시에 실행
    start_date=days_ago(1),           # DAG 최초 실행 기준
    catchup=False,
    tags=["spark", "s3", "parquet"],
)
def raw_to_swingdata_range_dag():

    # (선택) 전날 날짜 로깅
    @task()
    def log_date(yesterday: str):
        import logging
        logging.info(f"[INPUT] processing date: {yesterday}")

    # 전날 날짜 템플릿 변수
    date_template = "{{ macros.ds_add(ds, -1) }}"

    log_task = log_date(yesterday=date_template)

    # 공통 --conf 리스트
    common_conf = []
    for key, value in spark_configs.items():
        common_conf += ["--conf", f"{key}={value}"]
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
        "--start-date", date_template,
        "--end-date",   date_template,
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
            limits={"memory": "2Gi",   "cpu": "1000m"},
        ),
    )

    # Base -> SwingData SparkSubmit
    base_args = [
        "--master", api_server,
        "--deploy-mode", "cluster",
        "--name", f"{dag_name}-base",
        *common_conf,
        "s3a://creatz-airflow-jobs/base_to_swingdata/scripts/run_swingdata_extract_pipeline.py",
        "--start-date", date_template,
        "--end-date",   date_template,
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
        is_delete_operator_pod=True,
        service_account_name="airflow-irsa",
        image_pull_secrets=[V1LocalObjectReference(name="ecr-pull-secret")],
        node_selector={"intent": "spark"},
        container_resources=V1ResourceRequirements(
            requests={"memory": "1.5Gi", "cpu": "500m"},
            limits={"memory": "2Gi",   "cpu": "1000m"},
        ),
    )

    log_task >> raw_task >> base_task

# DAG 인스턴스화
raw_to_swingdata_range_dag_instance = raw_to_swingdata_range_dag()
