# Databricks notebook source
import unittest
from unittest.mock import patch, MagicMock
import api

class TestApi(unittest.TestCase):
    @patch("api.requests.get")
    @patch("api.SparkSession")
    def test_fetch_and_save_posts_success(self, mock_spark, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1, "title": "test"}]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_df = MagicMock()
        mock_spark.builder.appName.return_value.getOrCreate.return_value.createDataFrame.return_value = mock_df

        result = api.fetch_and_save_posts("https://fakeapi", "test_table")
        self.assertTrue(result)
        mock_df.write.format.assert_called_with("delta")

    @patch("api.requests.get", side_effect=api.requests.exceptions.RequestException("API error"))
    @patch("api.SparkSession")
    def test_fetch_and_save_posts_failure(self, mock_spark, mock_get):
        result = api.fetch_and_save_posts("https://fakeapi", "test_table")
        self.assertFalse(result)


# COMMAND ----------

if __name__ == "__main__":
    unittest.main()
