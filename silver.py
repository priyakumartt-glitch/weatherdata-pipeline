# Databricks notebook source

# 1. Read Bronze Data
df = spark.read.option("multiline", "true") \
    .json("file:/Workspace/Users/priyakumartt@gmail.com/bronze/*.json")

print("Bronze Rows:", df.count())
display(df)

# 2. Silver Transformation
from pyspark.sql.functions import col

silver_df = df.select(
    col("name").alias("city"),
    col("main.temp").alias("temperature"),
    col("main.humidity").alias("humidity"),
    col("main.pressure").alias("pressure"),
    col("main.temp_min").alias("temp_min"),
    col("main.temp_max").alias("temp_max"),
    col("wind.speed").alias("wind_speed"),
    col("wind.deg").alias("wind_deg"),
    col("clouds.all").alias("clouds_percent"),
    col("coord.lat").alias("latitude"),
    col("coord.lon").alias("longitude"),
    col("weather")[0]["main"].alias("weather_main"),
    col("weather")[0]["description"].alias("weather_description")
)

print("Silver Rows:", silver_df.count())
display(silver_df)

# 3. Save as CSV (Workspace)

silver_output_path = "file:/Workspace/Users/priyakumartt@gmail.com/silver/"

silver_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(silver_output_path)

print(" Silver CSV saved successfully")

# 4. Check output files

display(dbutils.fs.ls(silver_output_path))



