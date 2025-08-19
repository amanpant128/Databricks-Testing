# Databricks notebook source
import json
import requests
from pyspark.sql import SparkSession


# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("API to Table").getOrCreate()

# Use a simple, open API with basic JSON output
url = "https://jsonplaceholder.typicode.com/posts"  # Fake placeholder API for testing

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()  # This is already a list of dicts
    
    # Create DataFrame directly from list of dicts
    df = spark.createDataFrame(data)
    # df.show(5, truncate=False)
    df.display()
    
    # Save as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("placeholder_posts")
    print("Data saved to table 'placeholder_posts'")
except requests.exceptions.RequestException as e:
    print(f"Error fetching API data: {e}")

