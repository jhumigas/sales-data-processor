"""
Simple unit tests for Delta Loader.
"""

import pytest
import os
import tempfile
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sales_data_processor.loaders.delta_loader import DeltaLoader
from sales_data_processor.loaders.exceptions import LoadingError


class TestDeltaLoader:
    """Simple test cases for Delta Loader."""

    @pytest.fixture
    def loader(self, spark_session):
        """Create a default Delta loader instance."""
        return DeltaLoader(spark=spark_session)

    @pytest.fixture
    def custom_loader(self, spark_session):
        """Create a custom Delta loader with specific options."""
        return DeltaLoader(
            spark=spark_session,
            mode="append",
            options={"checkpointLocation": "/tmp/checkpoint"},
        )

    @pytest.fixture
    def sample_dataframe(self, spark_session):
        """Sample DataFrame for testing."""
        data = [
            ("Alice", 25, "New York"),
            ("Bob", 30, "Los Angeles"),
            ("Charlie", 35, "Chicago"),
        ]
        return spark_session.createDataFrame(data, ["name", "age", "city"])

    def test_load_basic_dataframe(
        self, spark_session, temp_dir, loader, sample_dataframe
    ):
        """Test loading a basic DataFrame to Delta Lake."""
        # Arrange
        delta_path = os.path.join(temp_dir, "test_delta_table")

        # Act
        loader.load(sample_dataframe, delta_path)

        # Assert - Verify the data was written by reading it back
        result_df = spark_session.read.format("delta").load(delta_path)
        assert result_df.count() == 3
        assert set(result_df.columns) == {"name", "age", "city"}

        # Verify specific data
        alice_data = result_df.filter(result_df.name == "Alice").collect()
        assert len(alice_data) == 1
        assert alice_data[0]["age"] == 25
        assert alice_data[0]["city"] == "New York"

    def test_load_as_table(self, spark_session, temp_dir, loader, sample_dataframe):
        """Test loading DataFrame as a Delta Lake table."""
        # Arrange
        table_name = "test_table"

        # Act
        loader.load_as_table(sample_dataframe, table_name)

        # Assert - Verify the table was created by reading it back
        result_df = spark_session.read.table(table_name)
        assert result_df.count() == 3
        assert set(result_df.columns) == {"name", "age", "city"}

    def test_load_as_table_with_database(
        self, spark_session, temp_dir, loader, sample_dataframe
    ):
        """Test loading DataFrame as a Delta Lake table with database."""
        # Arrange
        database = "test_db"
        table_name = "test_table"

        # Create database first
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        # Act
        loader.load_as_table(sample_dataframe, table_name, database)

        # Assert - Verify the table was created in the database
        result_df = spark_session.read.table(f"{database}.{table_name}")
        assert result_df.count() == 3
        assert set(result_df.columns) == {"name", "age", "city"}

    def test_load_empty_dataframe_raises_error(self, spark_session, temp_dir, loader):
        """Test that loading empty DataFrame raises LoadingError."""
        # Arrange
        empty_df = spark_session.createDataFrame([], StructType([]))
        delta_path = os.path.join(temp_dir, "empty_delta_table")

        # Act & Assert
        with pytest.raises(LoadingError) as exc_info:
            loader.load(empty_df, delta_path)

        assert "Cannot load empty DataFrame" in str(exc_info.value)

    def test_load_with_different_modes(self, spark_session, temp_dir, sample_dataframe):
        """Test loading with different write modes."""
        # Arrange
        delta_path = os.path.join(temp_dir, "mode_test_delta")

        # First load with overwrite mode
        loader1 = DeltaLoader(spark=spark_session, mode="overwrite")
        loader1.load(sample_dataframe, delta_path)

        # Verify initial data
        result1 = spark_session.read.format("delta").load(delta_path)
        assert result1.count() == 3

        # Load again with append mode
        loader2 = DeltaLoader(spark=spark_session, mode="append")
        loader2.load(sample_dataframe, delta_path)

        # Verify data was appended
        result2 = spark_session.read.format("delta").load(delta_path)
        assert result2.count() == 6  # 3 + 3

    def test_load_with_custom_options(
        self, spark_session, temp_dir, custom_loader, sample_dataframe
    ):
        """Test loading with custom options."""
        # Arrange
        delta_path = os.path.join(temp_dir, "custom_options_delta")

        # Act
        custom_loader.load(sample_dataframe, delta_path)

        # Assert - Verify the data was written
        result_df = spark_session.read.format("delta").load(delta_path)
        assert result_df.count() == 3
        assert set(result_df.columns) == {"name", "age", "city"}

    def test_load_error_handling(
        self, spark_session, temp_dir, loader, sample_dataframe
    ):
        """Test error handling during loading."""
        # Arrange - Use invalid path that should cause an error
        invalid_path = "/invalid/path/that/does/not/exist/delta_table"

        # Act & Assert
        with pytest.raises(LoadingError) as exc_info:
            loader.load(sample_dataframe, invalid_path)

        assert "Failed to load data to Delta Lake" in str(exc_info.value)
        assert invalid_path in str(exc_info.value)


class TestLoadingError:
    """Test cases for LoadingError exception."""

    def test_loading_error_message(self):
        """Test LoadingError exception message."""
        error_msg = "Test loading error"
        error = LoadingError(error_msg)
        assert str(error) == error_msg

    def test_loading_error_inheritance(self):
        """Test that LoadingError inherits from Exception."""
        error = LoadingError("Test error")
        assert isinstance(error, Exception)
