# Databricks notebook source

# GOLD LAYER 

from pyspark.sql.functions import avg, round, col, first

df = spark.read.option("multiline", "true") \
    .json("file:/Workspace/Users/priyakumartt@gmail.com/bronze/*.json")
    
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

gold_df = silver_df.groupBy("city").agg(
    round(avg("temperature") - 273.15, 2).alias("temp_celsius"),
    round(avg("humidity"), 2).alias("avg_humidity"),
    round(avg("pressure"), 2).alias("avg_pressure"),
    round(avg("wind_speed"), 2).alias("avg_wind_speed"),
    first("weather_main").alias("weather_main"),
    first("weather_description").alias("weather_description"),
    first("wind_deg").alias("wind_deg")
)

gold_df = gold_df.withColumn(
    "wind_percentage",
    round((col("avg_wind_speed") / 10) * 100, 2)
)

display(gold_df)


# SAVE GOLD CSV

gold_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("file:/Workspace/Users/priyakumartt@gmail.com/gold/")

print(" Gold CSV saved successfully")