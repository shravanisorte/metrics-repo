import os
import sys
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession

# Optional: add project folder to Python path if needed
#sys.path.insert(0, r"C:\Users\shravani.raju\Desktop\metrics_repo")

# Path to Deequ JAR file
JAR_PATH = r"C:\Users\shravani.raju\Desktop\metrics_repo\CaseStudy\deequ-2.0.11-spark-3.3.jar"

spark = SparkSession.builder \
    .appName("DataQualityChecks") \
    .config("spark.jars", JAR_PATH) \
    .getOrCreate()

# Load sample data
data = spark.read.csv(r"C:\Users\shravani.raju\Desktop\metrics_repo\CaseStudy\sample_sales_data.csv", header=True, inferSchema=True)

# Define quality rules
check = Check(spark, CheckLevel.Error, "Data quality checks") \
    .hasSize(lambda x: x >= 10000) \
    .isComplete("transaction_id") \
    .isNonNegative("sales_amount")

# Run verification
result = VerificationSuite(spark) \
    .onData(data) \
    .addCheck(check) \
    .run()

if result.status != "Success":
    print("Data quality checks failed. Blocking deployment.")
    exit(1)

print("Data quality checks passed.")
spark.stop()


# '''
# import os

# import sys
# from pydeequ.checks import Check, CheckLevel
# from pydeequ.verification import VerificationSuite
# from pyspark.sql import SparkSession

# # Set Spark version for PyDeequ
# os.environ["SPARK_VERSION"] = "3.3"

# # Add virtual environment's Scripts folder to PATH (for Windows)
# os.environ["PATH"] = r"C:\Users\shravani.raju\Desktop\metrics_repo\venv\Scripts;" + os.environ["PATH"]

# # Add project root to Python path
# sys.path.insert(0, r"C:\Users\shravani.raju\Desktop\metrics_repo")

# # Path to Deequ JAR
# JAR_PATH = r"C:\Users\shravani.raju\Desktop\metrics_repo\CaseStudy\deequ-2.0.11-spark-3.3.jar"

# # Initialize SparkSession with Deequ JAR
# spark = SparkSession.builder \
#     .appName("DataQualityChecks") \
#     .config("spark.jars", JAR_PATH) \
#     .getOrCreate()

# print(f"Using Deequ JAR: {JAR_PATH}")

# # Load sample data
# data = spark.read.csv(
#     r"C:\Users\shravani.raju\Desktop\metrics_repo\CaseStudy\sample_sales_data.csv",
#     header=True,
#     inferSchema=True
# )

# # Define quality rules
# check = Check(spark, CheckLevel.Error, "Data quality checks") \
#     .hasSize(lambda x: x >= 10000) \
#     .isComplete("transaction_id") \
#     .isNonNegative("sales_amount")

# # Run verification
# result = VerificationSuite(spark) \
#     .onData(data) \
#     .addCheck(check) \
#     .run()

# if str(result.status) != "Success":
#     print("Data quality checks failed. Blocking deployment.")
#     spark.stop()
#     sys.exit(1)

# print("Data quality checks passed.")
# spark.stop()
# '''
