import os
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from sales_data_processor.extractors.exceptions import ExtractionError
from sales_data_processor.extractors.base_extractor import IBaseExtractor


class CSVExtractor(IBaseExtractor):
    """Extractor for CSV files."""

    def __init__(
        self,
        spark: SparkSession,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize CSV extractor with configuration.

        Args:
            header: Whether CSV has header row
            infer_schema: Whether to infer schema automatically
            delimiter: Field delimiter character
            schema: Predefined schema (overrides infer_schema)
            options: Additional Spark CSV reader options
        """
        self.header = header
        self.infer_schema = infer_schema if schema is None else False
        self.delimiter = delimiter
        self.schema = schema
        self.options = options or {}
        self.spark = spark

    def extract(self, file_path: str) -> DataFrame:
        """
        Extract data from CSV file.

        Args:
            file_path: Path to CSV file

        Returns:
            DataFrame: Extracted data

        Raises:
            ExtractionError: If file doesn't exist or extraction fails
        """
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                raise ExtractionError(f"CSV file not found: {file_path}")

            # Build reader with base options
            reader = (
                self.spark.read.option("header", self.header)
                .option("inferSchema", self.infer_schema)
                .option("delimiter", self.delimiter)
            )

            # Add custom options
            for key, value in self.options.items():
                reader = reader.option(key, value)

            # Apply schema if provided
            if self.schema:
                reader = reader.schema(self.schema)

            # Read the CSV
            df = reader.csv(file_path)

            # Validate DataFrame is not empty
            if df.count() == 0:
                # raise warning instead of error? depends on use case
                raise ExtractionError(f"CSV file is empty: {file_path}")

            return df

        except Exception as e:
            if isinstance(e, ExtractionError):
                raise
            raise ExtractionError(f"Failed to extract CSV from {file_path}: {str(e)}")

    def extract_with_validation(
        self, file_path: str, expected_columns: Optional[list] = None, min_rows: int = 1
    ) -> DataFrame:
        """
        Extract CSV data with additional validation.

        Args:
            file_path: Path to CSV file
            expected_columns: List of expected column names
            min_rows: Minimum number of rows expected

        Returns:
            DataFrame: Extracted and validated data

        Raises:
            ExtractionError: If validation fails
        """
        # Extract the data first
        df = self.extract(file_path)

        # Validate columns if specified
        if expected_columns:
            missing_columns = set(expected_columns) - set(df.columns)
            if missing_columns:
                raise ExtractionError(
                    f"Missing expected columns: {', '.join(missing_columns)}. "
                    f"Found columns: {', '.join(df.columns)}"
                )

        # Validate row count
        row_count = df.count()
        if row_count < min_rows:
            raise ExtractionError(
                f"Insufficient rows in CSV file. Found {row_count}, expected at least {min_rows}"
            )

        return df
