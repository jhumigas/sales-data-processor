from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pydantic import BaseModel, Field
from loguru import logger

from sales_data_processor.processors.base_processor import BaseProcessor
from sales_data_processor.processors.exceptions import ProcessingError


class SalesColumns(BaseModel):
    """Column definitions for sales data."""

    invoice_no: str = Field(default="invoice_no", description="Unique invoice number")
    customer_id: str = Field(default="customer_id", description="Customer identifier")
    category: str = Field(default="category", description="Product category")
    quantity: str = Field(default="quantity", description="Quantity purchased")
    price: str = Field(default="price", description="Unit price")
    invoice_date: str = Field(default="invoice_date", description="Invoice date")
    shopping_mall: str = Field(
        default="shopping_mall", description="Shopping mall name"
    )

    def to_dict(self) -> Dict[str, str]:
        """Convert column definitions to dictionary."""
        return {
            "invoice_no": self.invoice_no,
            "customer_id": self.customer_id,
            "category": self.category,
            "quantity": self.quantity,
            "price": self.price,
            "invoice_date": self.invoice_date,
            "shopping_mall": self.shopping_mall,
        }

    def get_string_columns(self) -> list[str]:
        """Get list of string column names."""
        return [self.invoice_no, self.customer_id, self.category, self.shopping_mall]

    def get_numeric_columns(self) -> list[str]:
        """Get list of numeric column names."""
        return [self.quantity, self.price]

    def get_duplicate_key_columns(self) -> list[str]:
        """Get list of columns to use for duplicate detection."""
        return [self.invoice_no]


class SalesProcessor(BaseProcessor):
    """Processor for sales data with specific business rules."""

    def __init__(self, columns: Optional[SalesColumns] = None):
        """
        Initialize sales processor with column definitions and schema.

        Args:
            columns: Optional custom column definitions
        """
        self.columns = columns or SalesColumns()
        logger.info(
            f"Initializing SalesProcessor with columns: {list(self.columns.to_dict().keys())}"
        )

        schema = T.StructType(
            [
                T.StructField(self.columns.invoice_no, T.StringType(), True),
                T.StructField(self.columns.customer_id, T.StringType(), True),
                T.StructField(self.columns.category, T.StringType(), True),
                T.StructField(self.columns.quantity, T.IntegerType(), True),
                T.StructField(self.columns.price, T.DoubleType(), True),
                T.StructField(self.columns.invoice_date, T.DateType(), True),
                T.StructField(self.columns.shopping_mall, T.StringType(), True),
            ]
        )
        super().__init__(schema=schema)
        logger.debug("SalesProcessor initialized successfully")

    def process(self, df: DataFrame) -> DataFrame:
        """
        Process sales DataFrame with business rules.

        Args:
            df: Input sales DataFrame

        Returns:
            DataFrame: Processed sales DataFrame

        Raises:
            ProcessingError: If processing fails
        """
        initial_count = df.count()
        logger.info(f"Starting sales data processing with {initial_count} rows")

        try:
            # Apply schema first
            logger.debug("Step 1: Applying schema")
            processed_df = self.apply_schema(df)

            # Trim string columns
            logger.debug("Step 2: Trimming string columns")
            processed_df = self.trim_string_columns(
                processed_df, self.columns.get_string_columns()
            )

            # Handle missing values
            logger.debug("Step 3: Handling missing values")
            processed_df = self.handle_missing_values(
                processed_df,
                string_columns=[self.columns.category, self.columns.shopping_mall],
                numeric_columns=self.columns.get_numeric_columns(),
                default_string="Unknown",
                default_numeric=0.0,
            )

            # Process invoice date
            logger.debug("Step 4: Processing invoice dates")
            processed_df = self.process_invoice_date(processed_df)

            # Validate quantity and price
            logger.debug("Step 5: Validating numeric fields")
            processed_df = self.validate_numeric_fields(processed_df)

            # Clean category values
            logger.debug("Step 6: Cleaning category values")
            processed_df = self.clean_category(processed_df)

            # Clean shopping mall names
            logger.debug("Step 7: Cleaning shopping mall names")
            processed_df = self.clean_shopping_mall_names(processed_df)

            # Remove duplicates based on invoice_no
            logger.debug("Step 8: Removing duplicates")
            processed_df = self.remove_duplicates(
                processed_df, self.columns.get_duplicate_key_columns()
            )

            # Final validation
            logger.debug("Step 9: Final validation")
            processed_df = self.validate_sales_data(processed_df)

            final_count = processed_df.count()
            logger.info(
                f"Sales data processing completed successfully: {initial_count} → {final_count} rows"
            )

            return processed_df

        except Exception as e:
            logger.error(f"Failed to process sales data: {str(e)}")
            if isinstance(e, ProcessingError):
                raise
            raise ProcessingError(f"Failed to process sales data: {str(e)}")

    def process_invoice_date(self, df: DataFrame) -> DataFrame:
        """Process and convert invoice date to proper format."""
        logger.debug(
            f"Processing invoice dates in column '{self.columns.invoice_date}'"
        )

        # Count null dates before processing
        null_dates = df.filter(F.col(self.columns.invoice_date).isNull()).count()
        if null_dates > 0:
            logger.debug(f"Found {null_dates} null invoice dates")

        result_df = df.withColumn(
            self.columns.invoice_date,
            F.when(
                F.col(self.columns.invoice_date).isNotNull(),
                F.to_date(F.col(self.columns.invoice_date), "dd-MM-yyyy"),
            ).otherwise(None),
        )

        logger.debug("Invoice date processing completed")
        return result_df

    def validate_numeric_fields(self, df: DataFrame) -> DataFrame:
        """Validate quantity and price fields."""
        logger.debug("Validating numeric fields (quantity and price)")

        # Count invalid quantities before validation
        invalid_quantities = df.filter(F.col(self.columns.quantity) <= 0).count()
        if invalid_quantities > 0:
            logger.warning(
                f"Found {invalid_quantities} invalid quantities (≤ 0), setting to 1"
            )

        # Count invalid prices before validation
        invalid_prices = df.filter(F.col(self.columns.price) < 0).count()
        if invalid_prices > 0:
            logger.warning(
                f"Found {invalid_prices} invalid prices (< 0), setting to 0.0"
            )

        # Validate quantity (should be positive)
        result_df = df.withColumn(
            self.columns.quantity,
            F.when(F.col(self.columns.quantity) <= 0, 1).otherwise(
                F.col(self.columns.quantity)
            ),  # Minimum quantity of 1
        )

        # Validate price (should be non-negative)
        result_df = result_df.withColumn(
            self.columns.price,
            F.when(F.col(self.columns.price) < 0, 0.0).otherwise(
                F.col(self.columns.price)
            ),  # Price cannot be negative
        )

        logger.debug("Numeric field validation completed")
        return result_df

    def clean_category(self, df: DataFrame) -> DataFrame:
        """Clean and standardize category values."""
        logger.debug(f"Cleaning category values in column '{self.columns.category}'")

        # Count invalid categories before cleaning
        invalid_categories = df.filter(
            ~F.upper(F.trim(F.col(self.columns.category))).isin(
                [
                    "CLOTHING",
                    "SHOES",
                    "BOOKS",
                    "COSMETICS",
                    "FOOD & BEVERAGE",
                    "ELECTRONICS",
                    "ACCESSORIES",
                ]
            )
        ).count()

        if invalid_categories > 0:
            logger.info(
                f"Found {invalid_categories} invalid categories, setting to 'Other'"
            )
        else:
            logger.debug("All category values are valid")

        result_df = df.withColumn(
            self.columns.category,
            F.when(
                F.upper(F.trim(F.col(self.columns.category))).isin(
                    [
                        "CLOTHING",
                        "SHOES",
                        "BOOKS",
                        "COSMETICS",
                        "FOOD & BEVERAGE",
                        "ELECTRONICS",
                        "ACCESSORIES",
                    ]
                ),
                F.upper(F.trim(F.col(self.columns.category))),
            ).otherwise("Other"),
        )

        return result_df

    def clean_shopping_mall_names(self, df: DataFrame) -> DataFrame:
        """Clean shopping mall names by removing special characters and standardizing."""
        logger.debug(
            f"Cleaning shopping mall names in column '{self.columns.shopping_mall}'"
        )

        result_df = df.withColumn(
            self.columns.shopping_mall,
            F.regexp_replace(
                F.trim(F.col(self.columns.shopping_mall)), "[^a-zA-Z0-9\\s]", ""
            ),
        )

        logger.debug("Shopping mall name cleaning completed")
        return result_df

    def validate_sales_data(self, df: DataFrame) -> DataFrame:
        """Final validation of sales data."""
        initial_count = df.count()
        logger.debug(f"Performing final validation on {initial_count} sales records")

        result_df = df.filter(
            F.col(self.columns.invoice_no).isNotNull()
            & (F.col(self.columns.invoice_no) != "")
            & F.col(self.columns.customer_id).isNotNull()
            & (F.col(self.columns.customer_id) != "")
            & F.col(self.columns.quantity).isNotNull()
            & F.col(self.columns.price).isNotNull()
        )

        final_count = result_df.count()
        invalid_sales = initial_count - final_count

        if invalid_sales > 0:
            logger.warning(
                f"Filtered out {invalid_sales} records with invalid required fields"
            )
        else:
            logger.debug("All sales records have valid required fields")

        return result_df
