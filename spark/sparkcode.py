#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AverageBloodPressureByAgeRange") \
        .getOrCreate()
    
    # Read the CSV file
    # Adjust path as needed for your environment
    df = spark.read.option("header", "true").option("delimiter", "\t").csv("dataset.csv")
    
    # Define age ranges using when/otherwise
    df_with_age_range = df.withColumn(
        "age_range",
        when((col("Age") >= 18) & (col("Age") <= 30), "18-30")
        .when((col("Age") >= 31) & (col("Age") <= 45), "31-45")
        .when((col("Age") >= 46) & (col("Age") <= 60), "46-60")
        .when((col("Age") >= 61) & (col("Age") <= 75), "61-75")
        .otherwise("76+")
    )
    
    # Calculate average blood pressure by age range
    result = df_with_age_range.groupBy("age_range") \
        .agg(avg(col("BloodPressure")).alias("avg_blood_pressure")) \
        .select("age_range", col("avg_blood_pressure").cast("decimal(10,2)"))
    
    # Sort by age range for consistent output
    result = result.orderBy("age_range")
    
    # Show results
    result.show()
    
    # Save results to a CSV file (optional)
    # result.write.mode("overwrite").csv("output_avg_bp_by_age")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
