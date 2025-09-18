"""
Pytest-based unit tests for Parquet Extractor.
Uses fixtures from conftest.py for Spark session and test data management.
"""

import pytest
import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from chispa import assert_df_equality

from sales_data_processor.extractors.exceptions import ExtractionError
from sales_data_processor.extractors.parquet_extractor import ParquetExtractor


class TestParquetExtractor:
    """Test cases for Parquet Extractor using pytest."""

    @pytest.fixture
    def extractor(self, spark_session):
        """Create a default Parquet extractor instance."""
        return ParquetExtractor(spark=spark_session)

    @pytest.fixture
    def custom_extractor(self, spark_session):
        """Create a custom Parquet extractor with specific options."""
        return ParquetExtractor(
            spark=spark_session,
            options={"mergeSchema": "true", "recursiveFileLookup": "true"},
        )

    @pytest.fixture
    def sample_parquet_data(self, spark_session):
        """Sample Parquet data for testing."""
        data = [
            ("Alice", 25, "New York", 75000.50),
            ("Bob", 30, "Los Angeles", 85000.00),
            ("Charlie", 35, "Chicago", 95000.25),
            ("Diana", 28, "Miami", 70000.75),
        ]
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("salary", DoubleType(), True),
            ]
        )
        return spark_session.createDataFrame(data, schema)

    def write_parquet_file(self, spark_session, temp_dir, filename, df):
        """Helper method to write Parquet file."""
        file_path = os.path.join(temp_dir, filename)
        df.write.mode("overwrite").parquet(file_path)
        return file_path

    def test_extract_basic_parquet_file(
        self, spark_session, temp_dir, extractor, sample_parquet_data
    ):
        """Test extracting a basic Parquet file."""
        # Arrange
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "basic_test.parquet", sample_parquet_data
        )

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert
        assert result_df.count() == 4
        assert len(result_df.columns) == 4
        assert set(result_df.columns) == {"name", "age", "city", "salary"}

        # Verify specific data
        alice_data = result_df.filter(result_df.name == "Alice").collect()
        assert len(alice_data) == 1
        assert alice_data[0]["age"] == 25
        assert alice_data[0]["city"] == "New York"
        assert alice_data[0]["salary"] == 75000.50

    def test_extract_parquet_with_predefined_schema(
        self, spark_session, temp_dir, sample_parquet_data
    ):
        """Test extracting Parquet with predefined schema."""
        # Arrange
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "schema_test.parquet", sample_parquet_data
        )

        # Define a different schema (should still work if compatible)
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("salary", DoubleType(), True),
            ]
        )

        extractor = ParquetExtractor(spark=spark_session, schema=schema)

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert
        assert result_df.schema == schema

        # Verify data types
        alice_row = result_df.filter(result_df.name == "Alice").collect()[0]
        assert isinstance(alice_row["age"], int)
        assert isinstance(alice_row["salary"], float)
        assert alice_row["age"] == 25
        assert alice_row["salary"] == 75000.50

    def test_extract_parquet_with_custom_options(
        self, spark_session, temp_dir, custom_extractor, sample_parquet_data
    ):
        """Test extracting Parquet with custom options."""
        # Arrange
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "options_test.parquet", sample_parquet_data
        )

        # Act
        result_df = custom_extractor.extract(parquet_path)

        # Assert
        assert result_df.count() == 4
        assert set(result_df.columns) == {"name", "age", "city", "salary"}

    def test_extract_nonexistent_file_raises_error(self, extractor):
        """Test that extracting non-existent file raises ExtractionError."""
        # Arrange
        non_existent_path = "/path/that/does/not/exist.parquet"

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(non_existent_path)

        assert "Parquet file not found" in str(exc_info.value)
        assert non_existent_path in str(exc_info.value)

    def test_extract_with_validation_success(
        self, spark_session, temp_dir, extractor, sample_parquet_data
    ):
        """Test successful extraction with validation."""
        # Arrange
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "validation_test.parquet", sample_parquet_data
        )
        expected_columns = ["name", "age", "city", "salary"]

        # Act
        result_df = extractor.extract_with_validation(
            parquet_path, expected_columns=expected_columns, min_rows=3
        )

        # Assert
        assert result_df.count() == 4
        assert set(result_df.columns) == set(expected_columns)

    def test_extract_with_validation_missing_columns_fails(
        self, spark_session, temp_dir, extractor
    ):
        """Test extraction validation fails when expected columns are missing."""
        # Arrange
        limited_data = [("Alice", 25), ("Bob", 30)]
        limited_df = spark_session.createDataFrame(limited_data, ["name", "age"])
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "limited_cols.parquet", limited_df
        )
        expected_columns = ["name", "age", "department"]  # 'department' is missing

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract_with_validation(
                parquet_path, expected_columns=expected_columns
            )

        assert "Missing expected columns" in str(exc_info.value)
        assert "department" in str(exc_info.value)

    def test_extract_with_validation_insufficient_rows_fails(
        self, spark_session, temp_dir, extractor
    ):
        """Test extraction validation fails when insufficient rows."""
        # Arrange
        small_data = [("Alice", 25)]  # Only 1 data row
        small_df = spark_session.createDataFrame(small_data, ["name", "age"])
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "small_data.parquet", small_df
        )

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract_with_validation(
                parquet_path,
                min_rows=5,  # Expect at least 5 rows
            )

            assert "Insufficient rows" in str(exc_info.value)
            assert "found 1, expected at least 5" in str(exc_info.value)

    @pytest.mark.slow
    def test_extract_large_parquet_performance(
        self, spark_session, temp_dir, extractor
    ):
        """Test extracting large Parquet file for performance."""
        # Arrange - Create larger dataset
        large_data = []
        for i in range(10000):
            large_data.append(
                (str(i), f"name_{i}", float(i * 10), f"category_{i % 100}")
            )

        large_df = spark_session.createDataFrame(
            large_data, ["id", "name", "value", "category"]
        )
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "large_test.parquet", large_df
        )

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert
        assert result_df.count() == 10000
        assert len(result_df.columns) == 4

        # Verify some data integrity
        sample_rows = result_df.filter(
            result_df.id.isin(["0", "100", "9999"])
        ).collect()
        assert len(sample_rows) == 3

        # Check specific data
        first_row = [r for r in sample_rows if r["id"] == "0"][0]
        assert first_row["name"] == "name_0"
        assert first_row["value"] == 0.0
        assert first_row["category"] == "category_0"

    def test_extract_with_chispa_dataframe_comparison(
        self, spark_session, temp_dir, extractor
    ):
        """Test using chispa library for DataFrame comparison."""
        # Arrange
        test_data = [("Alice", 25), ("Bob", 30)]
        test_df = spark_session.createDataFrame(test_data, ["name", "age"])
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "chispa_test.parquet", test_df
        )

        # Create expected DataFrame
        expected_data = [("Alice", 25), ("Bob", 30)]
        expected_df = spark_session.createDataFrame(expected_data, ["name", "age"])

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert using chispa (more sophisticated DataFrame comparison)
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    @pytest.mark.integration
    def test_extract_real_world_parquet_structure(
        self, spark_session, temp_dir, extractor
    ):
        """Test with more realistic Parquet structure."""
        # Arrange - Create realistic sales data
        sales_data = [
            ("2024-01-01", "P001", "C001", 2, 29.99, 0.10),
            ("2024-01-01", "P002", "C002", 1, 149.99, 0.00),
            ("2024-01-02", "P001", "C003", 3, 29.99, 0.05),
            ("2024-01-02", "P003", "C001", 1, 89.99, 0.15),
        ]

        sales_df = spark_session.createDataFrame(
            sales_data,
            ["date", "product_id", "customer_id", "quantity", "unit_price", "discount"],
        )
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "sales_data.parquet", sales_df
        )

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert
        assert result_df.count() == 4
        assert "date" in result_df.columns
        assert "product_id" in result_df.columns

        # Verify data transformations work
        total_quantity = result_df.agg({"quantity": "sum"}).collect()[0][0]
        assert total_quantity == 7  # 2 + 1 + 3 + 1

        # Verify filtering works
        p001_sales = result_df.filter(result_df.product_id == "P001")
        assert p001_sales.count() == 2

    def test_extract_parquet_with_complex_schema(
        self, spark_session, temp_dir, extractor
    ):
        """Test extracting Parquet with complex nested schema."""
        # Arrange - Create DataFrame with complex types
        from pyspark.sql.types import ArrayType, MapType

        complex_data = [
            (
                "Alice",
                25,
                ["tag1", "tag2"],
                {"department": "Engineering", "level": "Senior"},
            ),
            ("Bob", 30, ["tag3"], {"department": "Sales", "level": "Junior"}),
        ]

        complex_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("tags", ArrayType(StringType()), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

        complex_df = spark_session.createDataFrame(complex_data, complex_schema)
        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "complex_schema.parquet", complex_df
        )

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert
        assert result_df.count() == 2
        assert set(result_df.columns) == {"name", "age", "tags", "metadata"}

        # Verify complex data types
        alice_row = result_df.filter(result_df.name == "Alice").collect()[0]
        assert alice_row["tags"] == ["tag1", "tag2"]
        assert alice_row["metadata"]["department"] == "Engineering"

    def test_extract_parquet_with_timestamp_data(
        self, spark_session, temp_dir, extractor
    ):
        """Test extracting Parquet with timestamp data."""
        # Arrange - Create DataFrame with timestamp
        from pyspark.sql.functions import to_timestamp

        timestamp_data = [
            ("Alice", "2024-01-01 10:30:00"),
            ("Bob", "2024-01-02 14:45:30"),
            ("Charlie", "2024-01-03 09:15:45"),
        ]

        timestamp_df = spark_session.createDataFrame(
            timestamp_data, ["name", "timestamp_str"]
        )
        timestamp_df = timestamp_df.withColumn(
            "timestamp", to_timestamp("timestamp_str", "yyyy-MM-dd HH:mm:ss")
        )
        timestamp_df = timestamp_df.select("name", "timestamp")

        parquet_path = self.write_parquet_file(
            spark_session, temp_dir, "timestamp_test.parquet", timestamp_df
        )

        # Act
        result_df = extractor.extract(parquet_path)

        # Assert
        assert result_df.count() == 3
        assert "timestamp" in result_df.columns

        # Verify timestamp data
        alice_row = result_df.filter(result_df.name == "Alice").collect()[0]
        assert alice_row["timestamp"] is not None


# Pytest fixtures that can be reused across test modules
@pytest.fixture
def parquet_test_data_factory(spark_session):
    """Factory fixture for creating different types of test Parquet data."""

    def _create_data(data_type):
        if data_type == "simple":
            data = [("Alice", 25), ("Bob", 30)]
            return spark_session.createDataFrame(data, ["name", "age"])
        elif data_type == "complex":
            data = [
                (
                    1,
                    "Alice Johnson",
                    "alice@example.com",
                    "Engineering",
                    85000.0,
                    "2023-01-15",
                ),
                (2, "Bob Smith", "bob@example.com", "Sales", 75000.0, "2023-02-01"),
                (
                    3,
                    "Charlie Brown",
                    "charlie@example.com",
                    "Marketing",
                    65000.0,
                    "2023-03-10",
                ),
            ]
            return spark_session.createDataFrame(
                data, ["id", "name", "email", "department", "salary", "start_date"]
            )
        elif data_type == "sales":
            data = [
                ("T001", "Laptop", 1, 999.99, "2024-01-01"),
                ("T002", "Mouse", 2, 29.99, "2024-01-02"),
                ("T003", "Keyboard", 1, 79.99, "2024-01-02"),
            ]
            return spark_session.createDataFrame(
                data, ["transaction_id", "product", "quantity", "price", "date"]
            )
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    return _create_data


# Performance and benchmark tests
# TODO: Add more performance tests for large datasets
