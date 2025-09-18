import abc
from pyspark.sql import DataFrame


class IBaseLoader(abc.ABC):
    """Base interface for data loaders."""

    @abc.abstractmethod
    def load(self, df: DataFrame, destination: str) -> None:
        """
        Load DataFrame to destination.

        Args:
            df: DataFrame to load
            destination: Destination path/table name

        Raises:
            LoadingError: If loading fails
        """
        pass
