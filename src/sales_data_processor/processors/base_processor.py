import abc
from typing import Optional, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from loguru import logger

from sales_data_processor.processors.exceptions import ProcessingError


class IBaseProcessor(abc.ABC):
    """Base interface for data processors."""

    @abc.abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        """
        Process DataFrame according to business rules.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Processed DataFrame

        Raises:
            ProcessingError: If processing fails
        """
        pass


class BaseProcessor(IBaseProcessor):
    """Base processor with common ETL operations."""

    def __init__(self, schema: Optional[T.StructType] = None):
        """
        Initialize base processor.

        Args:
            schema: Optional schema to apply to DataFrame
        """
        self.schema = schema

    def apply_schema(self, df: DataFrame) -> DataFrame:
        """Apply predefined schema to DataFrame."""
        if self.schema:
            logger.debug(
                f"Applying schema to DataFrame with {len(self.schema.fields)} fields"
            )
            result_df = df.select(
                *[
                    F.col(field.name).cast(field.dataType)
                    for field in self.schema.fields
                ]
            )
            logger.debug(
                f"Schema applied successfully. Result has {len(result_df.columns)} columns"
            )
            return result_df
        logger.debug("No schema defined, returning DataFrame as-is")
        return df

    def trim_string_columns(
        self, df: DataFrame, columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Trim whitespace from string columns."""
        if columns is None:
            # Get all string columns
            string_columns = [
                field.name
                for field in df.schema.fields
                if str(field.dataType) == "StringType()"
            ]
            logger.debug(f"Auto-detected {len(string_columns)} string columns to trim")
        else:
            string_columns = columns
            logger.debug(f"Trimming {len(string_columns)} specified string columns")

        result_df = df
        trimmed_count = 0
        for col_name in string_columns:
            if col_name in df.columns:
                result_df = result_df.withColumn(col_name, F.trim(F.col(col_name)))
                trimmed_count += 1
            else:
                logger.warning(f"Column '{col_name}' not found in DataFrame, skipping")

        logger.debug(f"Trimmed whitespace from {trimmed_count} columns")
        return result_df

    def handle_missing_values(
        self,
        df: DataFrame,
        string_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        default_string: str = "Unknown",
        default_numeric: float = 0.0,
    ) -> DataFrame:
        """Handle missing values in DataFrame."""
        logger.debug(
            f"Handling missing values: {len(string_columns or [])} string columns, {len(numeric_columns or [])} numeric columns"
        )
        result_df = df

        # Handle missing values in string columns
        if string_columns:
            for col_name in string_columns:
                if col_name in df.columns:
                    result_df = result_df.withColumn(
                        col_name,
                        F.when(
                            F.isnull(F.col(col_name)) | (F.col(col_name) == ""),
                            default_string,
                        ).otherwise(F.col(col_name)),
                    )
                    logger.debug(
                        f"Applied default string value '{default_string}' to column '{col_name}'"
                    )
                else:
                    logger.warning(f"String column '{col_name}' not found in DataFrame")

        # Handle missing values in numeric columns
        if numeric_columns:
            for col_name in numeric_columns:
                if col_name in df.columns:
                    result_df = result_df.withColumn(
                        col_name,
                        F.when(
                            F.isnull(F.col(col_name)) | F.isnan(F.col(col_name)),
                            default_numeric,
                        ).otherwise(F.col(col_name)),
                    )
                    logger.debug(
                        f"Applied default numeric value {default_numeric} to column '{col_name}'"
                    )
                else:
                    logger.warning(
                        f"Numeric column '{col_name}' not found in DataFrame"
                    )

        return result_df

    def remove_duplicates(
        self, df: DataFrame, subset: Optional[List[str]] = None
    ) -> DataFrame:
        """Remove duplicate rows from DataFrame."""
        initial_count = df.count()
        result_df = df.dropDuplicates(subset)
        final_count = result_df.count()
        duplicates_removed = initial_count - final_count

        if duplicates_removed > 0:
            logger.info(
                f"Removed {duplicates_removed} duplicate rows (from {initial_count} to {final_count})"
            )
        else:
            logger.debug("No duplicate rows found")

        return result_df
