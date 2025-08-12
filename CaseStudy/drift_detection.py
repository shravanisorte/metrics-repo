from scipy.stats import ks_2samp
import pandas as pd
from datetime import date

# Load data
baseline = pd.read_parquet("baseline_sales.parquet")
new_data = pd.read_parquet("sales_today.parquet")

# KS test
stat, p_value = ks_2samp(baseline['sales_amount'], new_data['sales_amount'])
drift_detected = p_value < 0.05

# Store results in Delta (Databricks example)
from delta import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{
    "date": str(date.today()),
    "column": "sales_amount",
    "drift_detected": drift_detected,
    "p_value": p_value
}])

df.write.format("delta").mode("append").save("/mnt/drift_metrics")
