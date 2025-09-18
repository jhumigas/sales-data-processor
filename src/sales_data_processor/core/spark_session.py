from loguru import logger
from typing import Optional, Dict
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip


class SparkSessionFactory:
    """Simple Spark session factory using singleton pattern."""

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(
        cls,
        app_name: str = "ETL_SparkApp",
    ) -> SparkSession:
        """
        Get or create a Spark session.

        Args:
            app_name: Name of the Spark application

        Returns:
            SparkSession: Spark session instance
        """
        if cls._instance is None:
            cls._instance = cls._create_session(app_name=app_name)
            logger.info(f"Created new Spark session: {app_name}")

        return cls._instance

    @classmethod
    def _create_session(cls, app_name: str) -> SparkSession:
        """Create a new Spark session with basic configuration."""
        spark_builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.ui.enabled", "false")
        )
        return configure_spark_with_delta_pip(spark_builder).getOrCreate()

    @classmethod
    def stop_session(cls) -> None:
        """Stop the current Spark session."""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
            logger.info("Spark session stopped")


def get_spark() -> SparkSession:
    """Get the default Spark session."""
    return SparkSessionFactory.get_session()


class SparkContext:
    """Context manager that ensures Spark session cleanup."""

    def __init__(self, app_name: str = "ETL_Application"):
        self.app_name = app_name

    def __enter__(self) -> SparkSession:
        return SparkSessionFactory.get_session(self.app_name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        SparkSessionFactory.stop_session()


# Example usage
if __name__ == "__main__":
    # Basic usage
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    # Using context manager
    with SparkContext("TestApp") as spark:
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.show()
