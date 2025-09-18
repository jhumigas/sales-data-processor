from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pydantic import BaseModel, Field
from loguru import logger

from sales_data_processor.processors.base_processor import BaseProcessor
from sales_data_processor.processors.exceptions import ProcessingError


class CustomerColumns(BaseModel):
    """Column definitions for customer data."""

    customer_id: str = Field(
        default="customer_id", description="Unique customer identifier"
    )
    gender: str = Field(default="gender", description="Customer gender")
    age: str = Field(default="age", description="Customer age")
    payment_method: str = Field(
        default="payment_method", description="Payment method used"
    )

    def to_dict(self) -> Dict[str, str]:
        """Convert column definitions to dictionary."""
        return {
            "customer_id": self.customer_id,
            "gender": self.gender,
            "age": self.age,
            "payment_method": self.payment_method,
        }

    def get_string_columns(self) -> list[str]:
        """Get list of string column names."""
        return [self.customer_id, self.gender, self.payment_method]

    def get_numeric_columns(self) -> list[str]:
        """Get list of numeric column names."""
        return [self.age]

    def get_duplicate_key_columns(self) -> list[str]:
        """Get list of columns to use for duplicate detection."""
        return [self.customer_id]


class CustomerProcessor(BaseProcessor):
    """Processor for customer data with specific business rules."""

    def __init__(self, columns: Optional[CustomerColumns] = None):
        """
        Initialize customer processor with column definitions and schema.

        Args:
            columns: Optional custom column definitions
        """
        self.columns = columns or CustomerColumns()
        logger.info(
            f"Initializing CustomerProcessor with columns: {list(self.columns.to_dict().keys())}"
        )

        schema = T.StructType(
            [
                T.StructField(self.columns.customer_id, T.StringType(), True),
                T.StructField(self.columns.gender, T.StringType(), True),
                T.StructField(self.columns.age, T.IntegerType(), True),
                T.StructField(self.columns.payment_method, T.StringType(), True),
            ]
        )
        super().__init__(schema=schema)
        logger.debug("CustomerProcessor initialized successfully")

    def process(self, df: DataFrame) -> DataFrame:
        """
        Process customer DataFrame with business rules.

        Args:
            df: Input customer DataFrame

        Returns:
            DataFrame: Processed customer DataFrame

        Raises:
            ProcessingError: If processing fails
        """
        initial_count = df.count()
        logger.info(f"Starting customer data processing with {initial_count} rows")

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
                string_columns=[self.columns.gender, self.columns.payment_method],
                numeric_columns=self.columns.get_numeric_columns(),
                default_string="Unknown",
                default_numeric=0,
            )

            # Validate and clean gender values
            logger.debug("Step 4: Validating and cleaning gender values")
            processed_df = self.validate_and_clean_gender(
                processed_df, self.columns.gender
            )

            # Validate age values (should be positive and reasonable)
            logger.debug("Step 5: Validating age values")
            processed_df = self.validate_age(processed_df)

            # Remove duplicates based on customer_id
            logger.debug("Step 6: Removing duplicates")
            processed_df = self.remove_duplicates(
                processed_df, self.columns.get_duplicate_key_columns()
            )

            # Final validation
            logger.debug("Step 7: Final validation")
            processed_df = self.validate_customer_data(processed_df)

            final_count = processed_df.count()
            logger.info(
                f"Customer data processing completed successfully: {initial_count} → {final_count} rows"
            )

            return processed_df

        except Exception as e:
            logger.error(f"Failed to process customer data: {str(e)}")
            if isinstance(e, ProcessingError):
                raise
            raise ProcessingError(f"Failed to process customer data: {str(e)}")

    def validate_age(self, df: DataFrame) -> DataFrame:
        """Validate age values (should be between 0 and 120)."""
        logger.debug(f"Validating age values in column '{self.columns.age}'")

        # Count invalid ages before validation
        invalid_ages = df.filter(
            F.col(self.columns.age).isNull()
            | (F.col(self.columns.age) < 0)
            | (F.col(self.columns.age) > 120)
        ).count()

        if invalid_ages > 0:
            logger.warning(
                f"Found {invalid_ages} invalid age values (null, < 0, or > 120), setting to 0"
            )
        else:
            logger.debug("All age values are valid")

        result_df = df.withColumn(
            self.columns.age,
            F.when(
                F.col(self.columns.age).isNull()
                | (F.col(self.columns.age) < 0)
                | (F.col(self.columns.age) > 120),
                0,
            ).otherwise(F.col(self.columns.age)),
        )

        return result_df

    def validate_and_clean_gender(
        self, df: DataFrame, gender_column: str = "gender"
    ) -> DataFrame:
        """Validate and clean gender column."""
        if gender_column not in df.columns:
            logger.warning(f"Gender column '{gender_column}' not found in DataFrame")
            return df

        logger.debug(
            f"Validating and cleaning gender values in column '{gender_column}'"
        )

        # Count invalid genders before validation
        invalid_genders = df.filter(
            ~F.upper(F.trim(F.col(gender_column))).isin(["MALE", "FEMALE"])
        ).count()

        if invalid_genders > 0:
            logger.info(
                f"Found {invalid_genders} invalid gender values, setting to 'Other'"
            )
        else:
            logger.debug("All gender values are valid")

        result_df = df.withColumn(
            gender_column,
            F.when(
                F.upper(F.trim(F.col(gender_column))).isin(["MALE", "FEMALE"]),
                F.upper(F.trim(F.col(gender_column))),
            ).otherwise("Other"),
        )

        return result_df

    def clean_customer_names(self, df: DataFrame, name_columns: List[str]) -> DataFrame:
        """Clean customer name columns by trimming and handling special characters."""
        result_df = df
        for col_name in name_columns:
            if col_name in df.columns:
                result_df = result_df.withColumn(
                    col_name,
                    F.regexp_replace(F.trim(F.col(col_name)), "[^a-zA-Z0-9\\s]", ""),
                )
        return result_df

    def validate_customer_data(self, df: DataFrame) -> DataFrame:
        """Final validation of customer data."""
        initial_count = df.count()
        logger.debug(f"Performing final validation on {initial_count} customer records")

        # Ensure customer_id is not null or empty
        result_df = df.filter(
            F.col(self.columns.customer_id).isNotNull()
            & (F.col(self.columns.customer_id) != "")
            & (F.trim(F.col(self.columns.customer_id)) != "")
        )

        final_count = result_df.count()
        invalid_customers = initial_count - final_count

        if invalid_customers > 0:
            logger.warning(
                f"Filtered out {invalid_customers} records with invalid customer_id"
            )
        else:
            logger.debug("All customer records have valid customer_id")

        return result_df
