from sales_data_processor.core.exceptions import ETLException


class ProcessingError(ETLException):
    """Exception raised during data processing."""

    pass
