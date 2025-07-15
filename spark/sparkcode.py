# Initialize Spark Session (Auto-configured for Dataproc + GCS)
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("HealthDataAnalysis") \
    .getOrCreate()

# 1. Load data from GCS
gcs_path = "gs://bidhan/home/health_data.csv"
df = spark.read.csv(gcs_path, header=True, inferSchema=True)

# 2. Data Preview
print("Schema:")
df.printSchema()
print("\nFirst 5 rows:")
df.show(5)

# 3. Data Processing
from pyspark.sql.functions import col, when, avg, count

processed_df = df.withColumn(
    "age_group",
    when(col("Age") < 30, "18-29")
    .when(col("Age") < 40, "30-39") 
    .when(col("Age") < 50, "40-49")
    .when(col("Age") < 60, "50-59")
    .otherwise("60+")
)

# 4. Analysis (Avg Blood Pressure by Age Group)
results = processed_df.groupBy("age_group") \
    .agg(
        avg("BloodPressure").alias("avg_blood_pressure"),
        count("*").alias("patient_count")
    ) \
    .orderBy("age_group")

# 5. Display Results
print("\nAverage Blood Pressure by Age Group:")
results.show()

# 6. Save Results to GCS
output_path = "gs://bidhan/output/health_analysis_results"
results.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"\nResults saved to: {output_path}")

# Optional: Plotting (requires pandas)
import matplotlib.pyplot as plt
pd_results = results.toPandas()
pd_results.plot(kind='bar', x='age_group', y='avg_blood_pressure', 
               title='Average Blood Pressure by Age Group')
plt.ylabel('Blood Pressure (mmHg)')
plt.show()

# Cleanup
spark.stop()
