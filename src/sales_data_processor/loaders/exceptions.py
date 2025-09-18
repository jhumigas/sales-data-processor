from sales_data_processor.core.exceptions import ETLException


class LoadingError(ETLException):
    """Exception raised during data loading."""

    pass
