import abc
from pyspark.sql import DataFrame


class IBaseExtractor(abc.ABC):
    @abc.abstractmethod
    def extract(self, source: str) -> DataFrame:
        pass
