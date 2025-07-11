from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_simple_dag',
    default_args=default_args,
    description='A simple Spark DAG with custom image',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'example'],
)

spark_task = SparkKubernetesOperator(
    task_id='run_simple_spark_job',
    name='simple-spark-job',
    namespace='airflow',
    template_spec={
        'apiVersion': 'sparkoperator.k8s.io/v1beta2',
        'kind': 'SparkApplication',
        'metadata': {
            'name': 'simple-spark-job',
            'namespace': 'airflow'
        },
        'spec': {
            'type': 'Python',
            'mode': 'cluster',
            'sparkVersion': '3.5.3',
            'image': '577638362884.dkr.ecr.us-west-2.amazonaws.com/aim/spark:3.5.3-python3.12.2-v4',
            'imagePullPolicy': 'Always',
            'mainApplicationFile': 's3a://creatz-airflow-jobs/test/scripts/test.py',
            'deps': {
                'pyFiles': [
                    'local:///opt/spark/deps/dependencies.zip'  # S3 → local로 변경
                ]
            },
            'restartPolicy': {
                'type': 'Never'
            },
            'sparkConf': {
                "spark.driver.extraJavaOptions": "-Djava.ext.dirs=/opt/spark/jars",
                "spark.executor.extraJavaOptions": "-Djava.ext.dirs=/opt/spark/jars",
                "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
                "spark.hadoop.fs.s3a.endpoint.region": "us-west-2",
                "spark.hadoop.fs.s3a.access.style": "PathStyle",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
            },
            'driver': {
                'cores': 1,
                'memory': '2g',
                'serviceAccount': 'airflow-irsa',
                'nodeSelector': {
                    'intent': 'spark'
                },
                'labels': {
                    'component': 'spark-driver'
                },
                'volumeMounts': [
                    {
                        'name': 'deps-volume',
                        'mountPath': '/opt/spark/deps'
                    }
                ]
            },
            'executor': {
                'cores': 1,
                'memory': '2g',
                'instances': 2,
                'nodeSelector': {
                    'intent': 'spark'
                },
                'labels': {
                    'component': 'spark-executor'
                },
                'volumeMounts': [
                    {
                        'name': 'deps-volume',
                        'mountPath': '/opt/spark/deps'
                    }
                ]
            },
            'volumes': [
                {
                    'name': 'deps-volume',
                    'emptyDir': {}
                }
            ],
            'initContainers': [
                {
                    'name': 'download-deps',
                    'image': 'amazonlinux:2',
                    'command': ['/bin/sh', '-c'],
                    'args': [
                        'yum install -y aws-cli > /dev/null && '
                        'aws s3 cp s3://creatz-airflow-jobs/test/zips/dependencies.zip /opt/spark/deps/dependencies.zip'
                    ],
                    'volumeMounts': [
                        {
                            'name': 'deps-volume',
                            'mountPath': '/opt/spark/deps'
                        }
                    ]
                }
            ]
        }
    },
    get_logs=True,
    do_xcom_push=False,
    success_run_history_limit=1,
    startup_timeout_seconds=600,
    log_events_on_failure=True,
    reattach_on_restart=True,
    delete_on_termination=True,
    kubernetes_conn_id='kubernetes_default',
    dag=dag,
)
