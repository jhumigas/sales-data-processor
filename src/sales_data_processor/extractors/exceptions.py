from sales_data_processor.core.exceptions import ETLException


class ExtractionError(ETLException):
    """Exception raised during data extraction."""

    pass
