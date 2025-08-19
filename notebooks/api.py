# Databricks notebook source
import requests
from pyspark.sql import SparkSession

# COMMAND ----------

def fetch_api_data(url):
    """
    Fetch data from the given API URL.
    Returns JSON data or None if request fails.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data: {e}")
        return None


# COMMAND ----------

def save_to_delta_table(data, table_name):
    """
    Save the provided data to a Delta table in Databricks.
    """
    spark = SparkSession.builder.appName("API to Table").getOrCreate()
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Data saved to table '{table_name}'")


# COMMAND ----------

# Example usage
if __name__ == "__main__":
    url = "https://jsonplaceholder.typicode.com/posts"
    data = fetch_api_data(url)
    if data:
        save_to_delta_table(data, "placeholder_posts")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit Tests

# COMMAND ----------

# Unit tests
def test_fetch_api_data_success():
    from unittest.mock import patch
    url = "https://jsonplaceholder.typicode.com/posts"
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = [{"id": 1}]
        mock_get.return_value.raise_for_status = lambda: None
        result = fetch_api_data(url)
        assert result == [{"id": 1}]


# COMMAND ----------

def test_fetch_api_data_failure():
    from unittest.mock import patch
    url = "https://jsonplaceholder.typicode.com/posts"
    with patch("requests.get", side_effect=Exception("fail")):
        result = fetch_api_data(url)
        assert result is None


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

