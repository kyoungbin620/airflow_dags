import json
import logging
from urllib.parse import urlparse
from datetime import timedelta
import os
import psycopg2

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ─────────────────────────────
# 설정/상수
# ─────────────────────────────
DAG_ID = "cleanup_expired_recent_shots"
PG_CONN_ID = "postgres_default" 
AWS_CONN_ID = "aws_default"
BATCH_SIZE = 500  # 한 루프당 가져올 레코드 수 (OOM 보호)
S3_DELETE_CHUNK_SIZE = 1000

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # 10분 주기
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # 동일 DAG 겹침(동시 실행) 방지
    tags=["cleanup", "s3", "gc"],
) as dag:

    @task(execution_timeout=timedelta(minutes=8))
    def run_cleanup_loop():
        logger = logging.getLogger("airflow.task")

        # 1. AWS S3 & DB 연결
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        pg_host = os.environ.get("VIEW_PG_HOST")
        if pg_host:
            logger.info("Using explicit environment variables for PostgreSQL connection.")
            conn = psycopg2.connect(
                host=pg_host,
                port=os.environ.get("VIEW_PG_PORT", "5432"),
                dbname=os.environ.get("VIEW_PG_DBNAME", "view_db"),
                user=os.environ.get("VIEW_PG_USER", "postgres"),
                password=os.environ.get("VIEW_PG_PASSWORD", ""),
                sslmode="prefer"
            )
        else:
            logger.info(f"Using Airflow PostgresHook via {PG_CONN_ID}.")
            pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
            conn = pg_hook.get_conn()

        conn.autocommit = False

        total_cleaned = 0
        loop_count = 0
        
        # 2. 적체 해소를 위한 While Loop 패턴
        while True:
            loop_count += 1
            logger.info(f"--- [Sweep {loop_count}] Fetching {BATCH_SIZE} EXPIRED rows... ---")

            with conn.cursor() as cur:
                # SKIP LOCKED로 동시성 이슈 방어
                cur.execute(f"""
                    SELECT id, left_video_path, right_video_path, swingtrace_path, shotinfo_ai_path, image_assets
                    FROM view.recent_shots
                    WHERE retention_status = 'EXPIRED'
                    LIMIT {BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                """)
                rows = cur.fetchall()

            if not rows:
                logger.info("✅ No more 'EXPIRED' rows. Sweeper terminating.")
                break

            expired_ids = []
            s3_keys_by_bucket = {}

            for r in rows:
                row_id, left, right, st, sa, img_assets = r
                expired_ids.append(row_id)
                
                paths_to_delete = [left, right, st, sa]

                # JSON 썸네일(image_assets) 경로 일체 추출
                if img_assets:
                    if isinstance(img_assets, str):
                        try:
                            img_assets = json.loads(img_assets)
                        except Exception:
                            pass
                    
                    if isinstance(img_assets, dict):
                        for asset_list in img_assets.values():
                            if isinstance(asset_list, list):
                                for item in asset_list:
                                    if isinstance(item, dict) and item.get("path"):
                                        paths_to_delete.append(item.get("path"))

                # Bucket / Key 파싱 (문자열인 것만, 그리고 제외할 원본 bucket_path는 위 리스트에 아예 없음)
                for path in paths_to_delete:
                    if path and isinstance(path, str) and path.startswith("s3://"):
                        parsed = urlparse(path)
                        bucket = parsed.netloc
                        key = parsed.path.lstrip("/")
                        if bucket and key:
                            s3_keys_by_bucket.setdefault(bucket, []).append(key)

            # 3. S3 일괄 삭제 (Batch)
            deletion_errors = False
            for bucket, keys in s3_keys_by_bucket.items():
                # 1000개 단위로 통신 (HTTP payload 크기는 단 몇 KB수준이라 행 절대 걸리지 않음)
                for i in range(0, len(keys), S3_DELETE_CHUNK_SIZE):
                    chunk = keys[i:i + S3_DELETE_CHUNK_SIZE]
                    try:
                        logger.info(f"🚀 [S3] Deleting {len(chunk)} objects from {bucket}...")
                        s3_hook.delete_objects(bucket=bucket, keys=chunk)
                    except Exception as e:
                        logger.error(f"❌ S3 deletion error on {bucket}: {e}")
                        deletion_errors = True
                        break
                
                if deletion_errors:
                    break

            if deletion_errors:
                logger.error("⚠️ Rolling back DB transaction to retry failed deletions next time.")
                conn.rollback()
                break

            # 4. S3 처리 성공 시 원자적 DB 삭제
            with conn.cursor() as cur:
                logger.info(f"🗑️ [DB] S3 cleanup is done. Hard deleting {len(expired_ids)} rows.")
                cur.execute(
                    "DELETE FROM view.recent_shots WHERE id = ANY(%s)",
                    (expired_ids,)
                )
            conn.commit()
            
            total_cleaned += len(expired_ids)
            logger.info(f"✨ Sub-total rows deleted this run: {total_cleaned}")

        conn.close()

    run_cleanup_task = run_cleanup_loop()
