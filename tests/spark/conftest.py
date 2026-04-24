"""Spark test fixtures — SparkSession for local testing."""

from __future__ import annotations

import os
import sys

# Set environment variables BEFORE importing PySpark
_python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = _python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = _python_exe
os.environ.setdefault("JAVA_HOME", r"D:\jdk-17")
os.environ.setdefault("HADOOP_HOME", r"D:\hadoop\hadoop-3.3.6")
os.environ.setdefault("SPARK_HOME", r"D:\spark\spark-3.5.5-bin-hadoop3")

import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("dbt-job-gen-test")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
