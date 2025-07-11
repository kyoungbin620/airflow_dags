from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder.appName("SimpleDependencyTest").getOrCreate()

try:
    # dependencies.zip에서 모듈 import
    from my_module import add

    result = add(3, 5)  # 간단한 덧셈 호출 예제
    print(f"모듈 my_module.add(3, 5)의 결과는: {result}")

    # 결과값을 Spark DataFrame으로 출력 (테스트 목적)
    df = spark.createDataFrame([(result,)], ["addition_result"])
    df.show()

except Exception as e:
    print(f"모듈 호출 중 에러가 발생했습니다: {e}")

finally:
    spark.stop()