from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 이전 DAG 실행 여부에 따라 실행 여부 결정 안 함
    'email_on_failure': False,  # 실패 시 이메일 알림 비활성화
    'email_on_retry': False,    # 재시도 시 이메일 알림 비활성화
    'retries': 1,               # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
}

# DAG 정의
with DAG(
    'simple_hello_world',  # DAG ID
    default_args=default_args,
    description='간단한 Hello World DAG 예시',
    schedule_interval=timedelta(days=1),  # 매일 실행
    start_date=datetime(2025, 5, 1),      # 시작일
    catchup=False,                         # 백필(false)
) as dag:

    # 1단계: 현재 날짜 출력
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
)

    # 2단계: 잠시 대기
t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
)

    # 3단계: Hello Airflow 출력
t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello Airflow!"',
)

    # 실행 순서 정의
t1 >> t2 >> t3
