#!/bin/bash

# 변수 설정
DEP_DIR="dependencies"
ZIP_FILE="dependencies.zip"
SCRIPT_FILE="test.py"
S3_BUCKET="s3://creatz-airflow-jobs/test"

# dependencies 폴더를 dependencies.zip로 압축
echo "압축 중: $DEP_DIR"
zip -r $ZIP_FILE $DEP_DIR

# dependencies.zip을 S3로 업로드
echo "S3에 dependencies.zip 업로드 중..."
aws s3 cp $ZIP_FILE $S3_BUCKET/zips/$ZIP_FILE

# run_raw_to_parquet.py 파일을 S3로 업로드
echo "S3에 run_raw_to_parquet.py 업로드 중..."
aws s3 cp $SCRIPT_FILE $S3_BUCKET/scripts/$SCRIPT_FILE

# 완료 메시지
echo "업로드 완료"
