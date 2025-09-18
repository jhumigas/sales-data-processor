"""
Simple unit tests for Customer Processor.
"""

import pytest
from pyspark.sql import types as T

from sales_data_processor.processors.customer_processor import CustomerProcessor


class TestCustomerProcessor:
    """Test cases for Customer Processor."""

    @pytest.fixture
    def processor(self):
        """Create a customer processor instance."""
        return CustomerProcessor()

    @pytest.fixture
    def sample_customer_data(self, spark_session):
        """Sample customer data for testing."""
        data = [
            ("C241288", "Female", 28, "Credit Card"),
            ("C111565", "Male", 21, "Debit Card"),
            ("C266599", "Male", 20, "Cash"),
            ("C988172", "Female", 66, "Credit Card"),
            ("C189076", "Female", 53, "Cash"),
            ("C999999", "", 25, "Unknown"),  # Empty gender
            ("C888888", "Invalid", 30, "Credit Card"),  # Invalid gender
            ("C777777", "Male", -5, "Debit Card"),  # Invalid age
            ("C666666", "Female", 150, "Cash"),  # Invalid age
            ("", "Male", 35, "Credit Card"),  # Empty customer_id
        ]
        schema = T.StructType(
            [
                T.StructField("customer_id", T.StringType(), True),
                T.StructField("gender", T.StringType(), True),
                T.StructField("age", T.IntegerType(), True),
                T.StructField("payment_method", T.StringType(), True),
            ]
        )
        return spark_session.createDataFrame(data, schema)

    def test_process_customer_data_basic(
        self, spark_session, processor, sample_customer_data
    ):
        """Test basic customer data processing."""
        # Act
        result_df = processor.process(sample_customer_data)

        # Assert
        assert result_df.count() == 9  # One empty customer_id should be filtered out
        assert set(result_df.columns) == {
            "customer_id",
            "gender",
            "age",
            "payment_method",
        }

        # Check that invalid ages were corrected
        invalid_ages = result_df.filter(
            (result_df.age < 0) | (result_df.age > 120)
        ).count()
        assert invalid_ages == 0

        # Check that invalid genders were set to "Other"
        other_genders = result_df.filter(result_df.gender == "Other").count()
        assert other_genders >= 1  # Should have at least the "Invalid" gender

    def test_process_handle_missing_gender(self, spark_session, processor):
        """Test handling of missing gender values."""
        # Arrange
        data = [
            ("C001", None, 25, "Credit Card"),
            ("C002", "", 30, "Debit Card"),
            ("C003", "Female", 35, "Cash"),
        ]
        df = spark_session.createDataFrame(
            data, ["customer_id", "gender", "age", "payment_method"]
        )

        # Act
        result_df = processor.validate_and_clean_gender(df, processor.columns.gender)

        # Assert
        # Missing genders should be set to "Unknown"
        unknown_genders = result_df.filter(result_df.gender == "Other").count()
        assert unknown_genders == 2

    def test_process_handle_invalid_ages(self, spark_session, processor):
        """Test handling of invalid age values."""
        # Arrange
        data = [
            ("C001", "Male", -10, "Credit Card"),
            ("C002", "Female", 200, "Debit Card"),
            ("C003", "Male", 25, "Cash"),
        ]
        df = spark_session.createDataFrame(
            data, ["customer_id", "gender", "age", "payment_method"]
        )

        # Act
        result_df = processor.validate_age(df)

        # Assert
        # Invalid ages should be set to 0
        zero_ages = result_df.filter(result_df.age == 0).count()
        assert zero_ages == 2

        # Valid age should remain
        valid_ages = result_df.filter(result_df.age == 25).count()
        assert valid_ages == 1

    def test_process_remove_duplicates(self, spark_session, processor):
        """Test removal of duplicate customer records."""
        # Arrange
        data = [
            ("C001", "Male", 25, "Credit Card"),
            ("C001", "Male", 25, "Credit Card"),  # Duplicate
            ("C002", "Female", 30, "Debit Card"),
            ("C001", "Male", 26, "Credit Card"),  # Same customer_id, different age
        ]
        df = spark_session.createDataFrame(
            data, ["customer_id", "gender", "age", "payment_method"]
        )

        # Act
        result_df = processor.remove_duplicates(
            df, processor.columns.get_duplicate_key_columns()
        )

        # Assert
        assert result_df.count() == 2  # Should have 2 unique customer_ids

    def test_process_trim_strings(self, spark_session, processor):
        """Test trimming of string values."""
        # Arrange
        data = [
            ("  C001  ", "  Male  ", 25, "  Credit Card  "),
            ("C002", "Female", 30, "Debit Card"),
        ]
        df = spark_session.createDataFrame(
            data, ["customer_id", "gender", "age", "payment_method"]
        )

        # Act
        result_df = processor.trim_string_columns(df)

        # Assert
        first_row = result_df.filter(result_df.customer_id == "C001").collect()[0]
        assert first_row["customer_id"] == "C001"
        assert first_row["gender"] == "Male"
        assert first_row["payment_method"] == "Credit Card"

    def test_process_schema_application(self, spark_session, processor):
        """Test that schema is properly applied."""
        # Arrange - Create DataFrame with different column order and types
        data = [
            ("C001", "25", "Male", "Credit Card"),  # age as string
        ]
        df = spark_session.createDataFrame(
            data, ["customer_id", "age", "gender", "payment_method"]
        )

        # Act
        result_df = processor.process(df)

        # Assert
        # Age should be converted to integer
        first_row = result_df.collect()[0]
        assert isinstance(first_row["age"], int)
        assert first_row["age"] == 25

    def test_process_error_handling(self, spark_session, processor):
        """Test error handling during processing."""
        # Arrange - Create DataFrame that might cause issues
        data = [("C001", "Male", 25, "Credit Card")]
        df = spark_session.createDataFrame(
            data, ["customer_id", "gender", "age", "payment_method"]
        )

        # Act - Should not raise exception for normal data
        result_df = processor.apply_schema(df)

        # Assert
        assert result_df.count() == 1
        assert result_df.collect()[0]["customer_id"] == "C001"
