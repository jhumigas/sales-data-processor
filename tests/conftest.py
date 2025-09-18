"""
Pytest configuration and fixtures for Spark ETL tests.
Enhanced version with additional fixtures for CSV extractor testing.
"""

import pytest
import tempfile
import shutil
import csv
import os
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.pip_utils import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for the entire test session.
    Configured for optimal testing performance.
    """
    spark_builder = (
        SparkSession.builder.appName("PyTestSession")
        .master("local[1]")  # Single thread for deterministic tests
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")  # Disable for predictable tests
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.execution.arrow.pyspark.enabled", "false"
        )  # Avoid Arrow issues in tests
    )

    spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Reduce log noise during tests
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_data():
    """Provide sample employee data for tests."""
    return [
        ("Alice", 25, "Engineering", 75000),
        ("Bob", 30, "Sales", 65000),
        ("Charlie", 35, "Engineering", 85000),
        ("Diana", 28, "Marketing", 60000),
    ]


@pytest.fixture
def sample_dataframe(spark_session, sample_data):
    """Create a sample Spark DataFrame from sample data."""
    columns = ["name", "age", "department", "salary"]
    return spark_session.createDataFrame(sample_data, columns)


@pytest.fixture
def csv_writer():
    """
    Fixture that provides a function to write CSV files.
    Returns a function that takes (file_path, data) and writes CSV.
    """

    def _write_csv(file_path: str, data: List[List[str]], **kwargs):
        """
        Write data to CSV file.

        Args:
            file_path: Path to write CSV file
            data: List of lists containing CSV data (including header)
            **kwargs: Additional arguments for csv.writer
        """
        with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, **kwargs)
            writer.writerows(data)
        return file_path

    return _write_csv


@pytest.fixture
def sample_csv_files(temp_dir, csv_writer):
    """
    Create various sample CSV files for testing.
    Returns a dictionary with file paths.
    """
    files = {}

    # Basic CSV with header
    basic_data = [
        ["name", "age", "city"],
        ["Alice", "25", "New York"],
        ["Bob", "30", "Los Angeles"],
        ["Charlie", "35", "Chicago"],
    ]
    files["basic"] = csv_writer(os.path.join(temp_dir, "basic.csv"), basic_data)

    # CSV without header
    no_header_data = [["Alice", "25", "New York"], ["Bob", "30", "Los Angeles"]]
    files["no_header"] = csv_writer(
        os.path.join(temp_dir, "no_header.csv"), no_header_data
    )

    # CSV with pipe delimiter
    pipe_data = [
        ["name", "age", "city"],
        ["Alice", "25", "New York"],
        ["Bob", "30", "Los Angeles"],
    ]
    pipe_file = os.path.join(temp_dir, "pipe_delimited.csv")
    with open(pipe_file, "w") as f:
        f.write("name|age|city\n")
        f.write("Alice|25|New York\n")
        f.write("Bob|30|Los Angeles\n")
    files["pipe"] = pipe_file

    # CSV with quotes and special characters
    complex_file = os.path.join(temp_dir, "complex.csv")
    with open(complex_file, "w") as f:
        f.write("name,description,notes\n")
        f.write('Alice,"A person, with comma","Has ""quotes"" in text"\n')
        f.write('Bob,"Another person","Simple text"\n')
    files["complex"] = complex_file

    # Empty CSV (only header)
    empty_data = [["name", "age", "city"]]
    files["empty"] = csv_writer(os.path.join(temp_dir, "empty.csv"), empty_data)

    # Large CSV for performance testing
    large_data = [["id", "name", "value"]]
    for i in range(1000):
        large_data.append([str(i), f"name_{i}", str(i * 10)])
    files["large"] = csv_writer(os.path.join(temp_dir, "large.csv"), large_data)

    yield files


@pytest.fixture
def spark_schemas():
    """Provide common Spark schemas for testing."""
    return {
        "basic": StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        ),
        "employee": StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        ),
        "sales": StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("product", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField(
                    "price", StringType(), True
                ),  # Keep as string for decimal handling
                StructField("date", StringType(), True),
            ]
        ),
    }


@pytest.fixture
def dataframe_factory(spark_session):
    """
    Factory fixture for creating test DataFrames.
    Returns a function that creates DataFrames from data and schema.
    """

    def _create_dataframe(
        data: List[tuple], columns: List[str], schema: StructType = None
    ):
        """
        Create a Spark DataFrame from data.

        Args:
            data: List of tuples containing row data
            columns: List of column names
            schema: Optional StructType schema

        Returns:
            DataFrame: Created Spark DataFrame
        """
        if schema:
            return spark_session.createDataFrame(data, schema)
        else:
            return spark_session.createDataFrame(data, columns)

    return _create_dataframe
