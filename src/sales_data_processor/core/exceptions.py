class ETLException(Exception):
    """Base exception for ETL operations."""

    pass


class ProcessingError(ETLException):
    """Exception raised during data processing."""

    pass
