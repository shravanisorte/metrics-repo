import os
import sys
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession
from pydeequ.checks import *

  # No () after Error

# Normalize JAR path for Windows
JAR_PATH = os.path.normpath(
    r"C:\Users\shravani.raju\Desktop\metrics_repo\CaseStudy\deequ-2.0.11-spark-3.3.jar"
)

os.environ["SPARK_VERSION"] = "3.3"
os.environ["PATH"] = r"C:\Users\shravani.raju\Desktop\metrics_repo\venv\Scripts;" + os.environ["PATH"]

spark = SparkSession.builder \
    .appName("DataQualityChecks") \
    .config("spark.jars", JAR_PATH) \
    .getOrCreate()

print(f" Spark started with Deequ JAR: {JAR_PATH}")

# Load CSV
data = spark.read.csv(
    r"C:\Users\shravani.raju\Desktop\metrics_repo\CaseStudy\sample_sales_data.csv",
    header=True,
    inferSchema=True
)
check = Check(spark, CheckLevel.Error, "Data quality checks") \
    .hasSize(lambda x: x >= 10000) \
    .isComplete("transaction_id") \
    .isNonNegative("sales_amount")

result = VerificationSuite(spark) \
    .onData(data) \
    .addCheck(check) \
    .run()

if str(result.status) != "Success":
    print(" Data quality checks failed.")
    spark.stop()
    sys.exit(1)

print(" Data quality checks passed.")
spark.stop()
