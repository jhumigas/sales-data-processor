"""
Simple unit tests for Sales Processor.
"""

import pytest
from pyspark.sql import types as T

from sales_data_processor.processors.sales_processor import SalesProcessor
from sales_data_processor.processors.exceptions import ProcessingError


class TestSalesProcessor:
    """Test cases for Sales Processor."""

    @pytest.fixture
    def processor(self):
        """Create a sales processor instance."""
        return SalesProcessor()

    @pytest.fixture
    def sample_sales_data(self, spark_session):
        """Sample sales data for testing."""
        data = [
            ("I138884", "C241288", "Clothing", 5, 1500.4, "05-08-2022", "Kanyon"),
            ("I317333", "C111565", "Shoes", 3, 1800.51, "12-12-2021", "Forum Istanbul"),
            ("I127801", "C266599", "Clothing", 1, 300.08, "09-11-2021", "Metrocity"),
            ("I173702", "C988172", "Shoes", 5, 3000.85, "16-05-2021", "Metropol AVM"),
            ("I337046", "C189076", "Books", 4, 60.6, "24-10-2021", "Kanyon"),
            (
                "I999999",
                "C999999",
                "Invalid Category",
                2,
                100.0,
                "01-01-2022",
                "Test Mall",
            ),
            (
                "I888888",
                "C888888",
                "Clothing",
                -1,
                50.0,
                "01-01-2022",
                "Test Mall",
            ),  # Invalid quantity
            (
                "I777777",
                "C777777",
                "Shoes",
                2,
                -10.0,
                "01-01-2022",
                "Test Mall",
            ),  # Invalid price
            (
                "",
                "C666666",
                "Books",
                1,
                25.0,
                "01-01-2022",
                "Test Mall",
            ),  # Empty invoice_no
        ]
        schema = T.StructType(
            [
                T.StructField("invoice_no", T.StringType(), True),
                T.StructField("customer_id", T.StringType(), True),
                T.StructField("category", T.StringType(), True),
                T.StructField("quantity", T.IntegerType(), True),
                T.StructField("price", T.DoubleType(), True),
                T.StructField("invoice_date", T.StringType(), True),
                T.StructField("shopping_mall", T.StringType(), True),
            ]
        )
        return spark_session.createDataFrame(data, schema)

    def test_process_sales_data_basic(
        self, spark_session, processor, sample_sales_data
    ):
        """Test basic sales data processing."""
        # Act
        result_df = processor.process(sample_sales_data)

        # Assert
        assert result_df.count() == 8  # One empty invoice_no should be filtered out
        assert set(result_df.columns) == {
            "invoice_no",
            "customer_id",
            "category",
            "quantity",
            "price",
            "invoice_date",
            "shopping_mall",
        }

        # Check that invalid quantities were corrected
        invalid_quantities = result_df.filter(result_df.quantity <= 0).count()
        assert invalid_quantities == 0

        # Check that invalid prices were corrected
        invalid_prices = result_df.filter(result_df.price < 0).count()
        assert invalid_prices == 0

    def test_process_handle_invalid_quantity(self, spark_session, processor):
        """Test handling of invalid quantity values."""
        # Arrange
        data = [
            ("I001", "C001", "Clothing", -5, 100.0, "01-01-2022", "Mall A"),
            ("I002", "C002", "Shoes", 0, 200.0, "01-01-2022", "Mall B"),
            ("I003", "C003", "Books", 3, 50.0, "01-01-2022", "Mall C"),
        ]
        df = spark_session.createDataFrame(
            data,
            [
                "invoice_no",
                "customer_id",
                "category",
                "quantity",
                "price",
                "invoice_date",
                "shopping_mall",
            ],
        )

        # Act
        result_df = processor.validate_numeric_fields(df)

        # Assert
        # Invalid quantities should be set to 1
        min_quantities = result_df.filter(result_df.quantity == 1).count()
        assert min_quantities == 2

        # Valid quantity should remain
        valid_quantities = result_df.filter(result_df.quantity == 3).count()
        assert valid_quantities == 1

    def test_process_handle_invalid_price(self, spark_session, processor):
        """Test handling of invalid price values."""
        # Arrange
        data = [
            ("I001", "C001", "Clothing", 2, -100.0, "01-01-2022", "Mall A"),
            ("I002", "C002", "Shoes", 1, 0.0, "01-01-2022", "Mall B"),
            ("I003", "C003", "Books", 3, 50.0, "01-01-2022", "Mall C"),
        ]
        df = spark_session.createDataFrame(
            data,
            [
                "invoice_no",
                "customer_id",
                "category",
                "quantity",
                "price",
                "invoice_date",
                "shopping_mall",
            ],
        )

        # Act
        result_df = processor.validate_numeric_fields(df)

        # Assert
        # Negative price should be set to 0
        zero_prices = result_df.filter(result_df.price == 0.0).count()
        assert zero_prices == 2

        # Valid price should remain
        valid_prices = result_df.filter(result_df.price == 50.0).count()
        assert valid_prices == 1

    def test_process_clean_category(self, spark_session, processor):
        """Test cleaning of category values."""
        # Arrange
        data = [
            ("I001", "C001", "clothing", 2, 100.0, "01-01-2022", "Mall A"),
            ("I002", "C002", "SHOES", 1, 200.0, "01-01-2022", "Mall B"),
            ("I003", "C003", "Invalid Category", 3, 50.0, "01-01-2022", "Mall C"),
        ]
        df = spark_session.createDataFrame(
            data,
            [
                "invoice_no",
                "customer_id",
                "category",
                "quantity",
                "price",
                "invoice_date",
                "shopping_mall",
            ],
        )

        # Act
        result_df = processor.clean_category(df)

        # Assert
        categories = [row["category"] for row in result_df.collect()]
        assert "CLOTHING" in categories
        assert "SHOES" in categories
        assert "Other" in categories

    def test_process_remove_duplicates(self, spark_session, processor):
        """Test removal of duplicate sales records."""
        # Arrange
        data = [
            ("I001", "C001", "Clothing", 2, 100.0, "01-01-2022", "Mall A"),
            ("I001", "C001", "Clothing", 2, 100.0, "01-01-2022", "Mall A"),  # Duplicate
            ("I002", "C002", "Shoes", 1, 200.0, "01-01-2022", "Mall B"),
        ]
        df = spark_session.createDataFrame(
            data,
            [
                "invoice_no",
                "customer_id",
                "category",
                "quantity",
                "price",
                "invoice_date",
                "shopping_mall",
            ],
        )

        # Act
        result_df = processor.remove_duplicates(
            df, processor.columns.get_duplicate_key_columns()
        )

        # Assert
        assert result_df.count() == 2  # Should have 2 unique invoice_nos

    def test_process_trim_strings(self, spark_session, processor):
        """Test trimming of string values."""
        # Arrange
        data = [
            (
                "  I001  ",
                "  C001  ",
                "  Clothing  ",
                2,
                100.0,
                "01-01-2022",
                "  Mall A  ",
            ),
        ]
        df = spark_session.createDataFrame(
            data,
            [
                "invoice_no",
                "customer_id",
                "category",
                "quantity",
                "price",
                "invoice_date",
                "shopping_mall",
            ],
        )

        # Act
        result_df = processor.trim_string_columns(df)

        # Assert
        first_row = result_df.collect()[0]
        assert first_row["invoice_no"] == "I001"
        assert first_row["customer_id"] == "C001"
        assert first_row["category"] == "Clothing"
        assert first_row["shopping_mall"] == "Mall A"

    def test_process_schema_application(self, spark_session, processor):
        """Test that schema is properly applied."""
        # Arrange - Create DataFrame with different types
        data = [
            (
                "I001",
                "C001",
                "Clothing",
                "2",
                "100.0",
                "01-01-2022",
                "Mall A",
            ),  # quantity and price as strings
        ]
        df = spark_session.createDataFrame(
            data,
            [
                "invoice_no",
                "customer_id",
                "category",
                "quantity",
                "price",
                "invoice_date",
                "shopping_mall",
            ],
        )

        # Act
        result_df = processor.apply_schema(df)

        # Assert
        first_row = result_df.collect()[0]
        assert isinstance(first_row["price"], float)
        assert first_row["quantity"] == 2
        assert first_row["price"] == 100.0
