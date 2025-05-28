from airflow.models import Variable
from airflow.utils.helpers import chain

from airflow.operators.bash import BashOperator  # 단순 확인용

with DAG(
    dag_id=dag_name,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_submit = KubernetesPodOperator(
        task_id="run_spark_submit_s3_script",
        name="spark-submit-s3-script",
        namespace="airflow",
        image=spark_image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://BFDDB67D4B8EC345DED44952FE9F1F9B.gr7.us-west-2.eks.amazonaws.com",
            "--deploy-mode", "cluster",
            "--name", dag_name,
            "--conf", "spark.kubernetes.namespace=airflow",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=airflow-irsa",
            "--conf", "spark.kubernetes.container.image.pullSecrets=ecr-pull-secret",
            "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
            "--conf", "spark.executor.instances=1",
            "--conf", "spark.executor.memory=512m",
            "--conf", "spark.executor.cores=1",
            "--conf", f"spark.ui.proxyBase=/spark-ui/{dag_name}",
            "--conf", f"spark.kubernetes.driver.label.spark-ui-selector={dag_name}",
            "--conf", "spark.kubernetes.executor.deleteOnTermination=true",
            "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
            "--conf", f"spark.kubernetes.container.image={spark_image}",
            "--conf", f"spark.kubernetes.driver.container.image={spark_image}",
            "s3a://creatz-aim-members/kbjin/monitoring_logs_to_parquet_daily.py",
            "--start-date", "{{ dag_run.conf['start_date'] | default('2025-05-01') }}",
            "--end-date", "{{ dag_run.conf['end_date'] | default('2025-05-02') }}"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name="airflow-irsa",
        image_pull_secrets=[k8s.V1LocalObjectReference(name="ecr-pull-secret")],
        container_resources=V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "500m"},
            limits={"memory": "2Gi", "cpu": "1000m"},
        )
    )

    spark_submit.template_fields = ("arguments",)
