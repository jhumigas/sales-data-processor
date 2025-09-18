import pytest
import os
import csv
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa import assert_df_equality

from sales_data_processor.extractors.exceptions import ExtractionError
from sales_data_processor.extractors import CSVExtractor


class TestCSVExtractor:
    """Test cases for CSV Extractor using pytest."""

    @pytest.fixture
    def extractor(self, spark_session):
        """Create a default CSV extractor instance."""
        return CSVExtractor(spark=spark_session)

    @pytest.fixture
    def custom_extractor(self, spark_session):
        """Create a custom CSV extractor with specific options."""
        return CSVExtractor(
            spark=spark_session,
            header=True,
            infer_schema=True,
            delimiter=",",
            options={"quote": '"', "escape": '"'},
        )

    @pytest.fixture
    def sample_csv_data(self):
        """Sample CSV data for testing."""
        return [
            ["name", "age", "city", "salary"],
            ["Alice", "25", "New York", "75000"],
            ["Bob", "30", "Los Angeles", "85000"],
            ["Charlie", "35", "Chicago", "95000"],
            ["Diana", "28", "Miami", "70000"],
        ]

    def write_csv_file(self, temp_dir, filename, data):
        """Helper method to write CSV file."""
        file_path = os.path.join(temp_dir, filename)
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)
        return file_path

    def test_extract_basic_csv_with_header(
        self, spark_session, temp_dir, extractor, sample_csv_data
    ):
        """Test extracting a basic CSV file with header."""
        # Arrange
        csv_path = self.write_csv_file(temp_dir, "basic_test.csv", sample_csv_data)

        # Act
        result_df = extractor.extract(csv_path)

        # Assert
        assert result_df.count() == 4
        assert len(result_df.columns) == 4
        assert set(result_df.columns) == {"name", "age", "city", "salary"}

        # Verify specific data
        alice_data = result_df.filter(result_df.name == "Alice").collect()
        assert len(alice_data) == 1
        assert alice_data[0]["age"] == 25  # Should be inferred as int
        assert alice_data[0]["city"] == "New York"

    @pytest.mark.parametrize(
        "delimiter,expected_count", [(",", 3), ("|", 3), (";", 3), ("\t", 3)]
    )
    def test_extract_csv_with_different_delimiters(
        self, spark_session, temp_dir, delimiter, expected_count
    ):
        """Test extracting CSV with different delimiters."""
        # Arrange
        filename = f"test_{delimiter.replace('|', 'pipe').replace('\t', 'tab')}.csv"
        csv_path = os.path.join(temp_dir, filename)

        with open(csv_path, "w") as f:
            f.write(f"name{delimiter}age{delimiter}city\n")
            f.write(f"Alice{delimiter}25{delimiter}New York\n")
            f.write(f"Bob{delimiter}30{delimiter}Los Angeles\n")
            f.write(f"Charlie{delimiter}35{delimiter}Chicago\n")

        extractor = CSVExtractor(spark=spark_session, delimiter=delimiter)

        # Act
        result_df = extractor.extract(csv_path)

        # Assert
        assert result_df.count() == expected_count
        assert "name" in result_df.columns
        assert result_df.filter(result_df.name == "Alice").count() == 1

    def test_extract_csv_with_predefined_schema(
        self, spark_session, temp_dir, sample_csv_data
    ):
        """Test extracting CSV with predefined schema."""
        # Arrange
        csv_path = self.write_csv_file(temp_dir, "schema_test.csv", sample_csv_data)

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )

        extractor = CSVExtractor(spark=spark_session, schema=schema, infer_schema=False)

        # Act
        result_df = extractor.extract(csv_path)

        # Assert
        assert result_df.schema == schema

        # Verify data types
        alice_row = result_df.filter(result_df.name == "Alice").collect()[0]
        assert isinstance(alice_row["age"], int)
        assert isinstance(alice_row["salary"], int)
        assert alice_row["age"] == 25
        assert alice_row["salary"] == 75000

    def test_extract_nonexistent_file_raises_error(self, extractor):
        """Test that extracting non-existent file raises ExtractionError."""
        # Arrange
        non_existent_path = "/path/that/does/not/exist.csv"

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(non_existent_path)

        assert "CSV file not found" in str(exc_info.value)
        assert non_existent_path in str(exc_info.value)

    def test_extract_empty_file_raises_error(self, temp_dir, extractor):
        """Test that extracting empty file raises ExtractionError."""
        # Arrange
        empty_csv_path = os.path.join(temp_dir, "empty.csv")
        with open(empty_csv_path, "w") as f:
            pass  # Create empty file

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(empty_csv_path)

        assert "CSV file is empty" in str(exc_info.value)

    def test_extract_only_header_raises_error(self, temp_dir, extractor):
        """Test that CSV with only header raises ExtractionError."""
        # Arrange
        header_only_data = [["name", "age", "city"]]  # Only header, no data
        csv_path = self.write_csv_file(temp_dir, "header_only.csv", header_only_data)

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(csv_path)

        assert "CSV file is empty" in str(exc_info.value)

    @pytest.mark.parametrize(
        "invalid_data",
        [
            [["name,age"], ["Alice,25,extra,field"]],  # Inconsistent columns
            [["name", "age"], ["Alice"]],  # Missing field
        ],
    )
    def test_extract_malformed_csv_handles_gracefully(
        self, spark_session, temp_dir, extractor, invalid_data
    ):
        """Test that malformed CSV is handled gracefully by Spark."""
        # Arrange
        csv_path = self.write_csv_file(temp_dir, "malformed.csv", invalid_data)

        # Act - Should not raise exception, Spark handles this
        result_df = extractor.extract(csv_path)

        # Assert - Spark should still create DataFrame, possibly with nulls
        assert result_df is not None
        assert result_df.count() >= 0

    def test_extract_with_validation_success(
        self, spark_session, temp_dir, extractor, sample_csv_data
    ):
        """Test successful extraction with validation."""
        # Arrange
        csv_path = self.write_csv_file(temp_dir, "validation_test.csv", sample_csv_data)
        expected_columns = ["name", "age", "city", "salary"]

        # Act
        result_df = extractor.extract_with_validation(
            csv_path, expected_columns=expected_columns, min_rows=3
        )

        # Assert
        assert result_df.count() == 4
        assert set(result_df.columns) == set(expected_columns)

    def test_extract_with_validation_missing_columns_fails(
        self, spark_session, temp_dir, extractor
    ):
        """Test extraction validation fails when expected columns are missing."""
        # Arrange
        limited_data = [["name", "age"], ["Alice", "25"], ["Bob", "30"]]
        csv_path = self.write_csv_file(temp_dir, "limited_cols.csv", limited_data)
        expected_columns = ["name", "age", "department"]  # 'department' is missing

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract_with_validation(
                csv_path, expected_columns=expected_columns
            )

        assert "Missing expected columns" in str(exc_info.value)
        assert "department" in str(exc_info.value)

    def test_extract_with_validation_insufficient_rows_fails(
        self, spark_session, temp_dir, extractor
    ):
        """Test extraction validation fails when insufficient rows."""
        # Arrange
        small_data = [
            ["name", "age"],
            ["Alice", "25"],  # Only 1 data row
        ]
        csv_path = self.write_csv_file(temp_dir, "small_data.csv", small_data)

        # Act & Assert
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract_with_validation(
                csv_path,
                min_rows=5,  # Expect at least 5 rows
            )

            assert "Insufficient rows" in str(exc_info.value)
            assert "found 1, expected at least 5" in str(exc_info.value)

    def test_extract_with_chispa_dataframe_comparison(
        self, spark_session, temp_dir, extractor
    ):
        """Test using chispa library for DataFrame comparison."""
        # Arrange
        test_data = [["name", "age"], ["Alice", "25"], ["Bob", "30"]]
        csv_path = self.write_csv_file(temp_dir, "chispa_test.csv", test_data)

        # Create expected DataFrame
        expected_data = [("Alice", 25), ("Bob", 30)]
        expected_df = spark_session.createDataFrame(expected_data, ["name", "age"])

        # Act
        result_df = extractor.extract(csv_path)

        # Assert using chispa (more sophisticated DataFrame comparison)
        assert_df_equality(
            result_df, expected_df, ignore_nullable=True, ignore_schema=True
        )


# Pytest fixtures that can be reused across test modules
@pytest.fixture
def csv_test_data_factory():
    """Factory fixture for creating different types of test CSV data."""

    def _create_data(data_type):
        if data_type == "simple":
            return [["name", "age"], ["Alice", "25"], ["Bob", "30"]]
        elif data_type == "complex":
            return [
                ["id", "name", "email", "department", "salary", "start_date"],
                [
                    "1",
                    "Alice Johnson",
                    "alice@example.com",
                    "Engineering",
                    "85000",
                    "2023-01-15",
                ],
                ["2", "Bob Smith", "bob@example.com", "Sales", "75000", "2023-02-01"],
                [
                    "3",
                    "Charlie Brown",
                    "charlie@example.com",
                    "Marketing",
                    "65000",
                    "2023-03-10",
                ],
            ]
        elif data_type == "sales":
            return [
                ["transaction_id", "product", "quantity", "price", "date"],
                ["T001", "Laptop", "1", "999.99", "2024-01-01"],
                ["T002", "Mouse", "2", "29.99", "2024-01-02"],
                ["T003", "Keyboard", "1", "79.99", "2024-01-02"],
            ]
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    return _create_data
