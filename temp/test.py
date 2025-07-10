from pyspark.sql import SparkSession
import random

def estimate_pi(n):
    count = 0
    for _ in range(n):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1:
            count += 1
    return 4.0 * count / n

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark Pi").getOrCreate()

    n = 1000000  # 샘플 개수 (늘릴수록 정밀도 증가)
    pi_estimate = estimate_pi(n)

    print(f"Estimated value of Pi is {pi_estimate}")

    spark.stop()

