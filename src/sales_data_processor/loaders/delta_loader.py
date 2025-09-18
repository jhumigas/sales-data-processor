from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from sales_data_processor.loaders.exceptions import LoadingError
from sales_data_processor.core.spark_session import get_spark
from sales_data_processor.loaders.base_loader import IBaseLoader


class DeltaLoader(IBaseLoader):
    """Loader for Delta Lake tables."""

    def __init__(
        self,
        spark: SparkSession,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize Delta loader with configuration.

        Args:
            mode: Write mode (overwrite, append, ignore, errorifexists)
            options: Additional Delta Lake writer options
        """
        self.mode = mode
        self.options = options or {}
        self.spark = spark

    def load(self, df: DataFrame, destination: str) -> None:
        """
        Load DataFrame to Delta Lake table.

        Args:
            df: DataFrame to load
            destination: Delta Lake table path or table name

        Raises:
            LoadingError: If loading fails
        """
        try:
            # Validate DataFrame is not empty
            if df.count() == 0:
                raise LoadingError(f"Cannot load empty DataFrame to {destination}")

            # Build writer with base options
            writer = df.write.mode(self.mode)

            # Add custom options
            for key, value in self.options.items():
                writer = writer.option(key, value)

            # Write to Delta Lake
            writer.format("delta").save(destination)

        except Exception as e:
            if isinstance(e, LoadingError):
                raise
            raise LoadingError(
                f"Failed to load data to Delta Lake {destination}: {str(e)}"
            )

    def load_as_table(
        self, df: DataFrame, table_name: str, database: Optional[str] = None
    ) -> None:
        """
        Load DataFrame as a Delta Lake table.

        Args:
            df: DataFrame to load
            table_name: Name of the table
            database: Optional database name

        Raises:
            LoadingError: If loading fails
        """
        try:
            # Validate DataFrame is not empty
            if df.count() == 0:
                raise LoadingError(f"Cannot load empty DataFrame as table {table_name}")

            # Build writer with base options
            writer = df.write.mode(self.mode)

            # Add custom options
            for key, value in self.options.items():
                writer = writer.option(key, value)

            # Create full table name
            full_table_name = f"{database}.{table_name}" if database else table_name

            # Write to Delta Lake table
            writer.format("delta").saveAsTable(full_table_name)

        except Exception as e:
            if isinstance(e, LoadingError):
                raise
            raise LoadingError(
                f"Failed to load data as Delta Lake table {full_table_name}: {str(e)}"
            )
