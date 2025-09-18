from loguru import logger
from sales_data_processor.config import ProjectConfig
from sales_data_processor.core.spark_session import SparkSessionFactory
from sales_data_processor.extractors import CSVExtractor, ParquetExtractor
from sales_data_processor.loaders import DeltaLoader
from sales_data_processor.processors import (
    CustomerProcessor,
    SalesProcessor,
    CustomerColumns,
    SalesColumns,
)


def main() -> None:
    logger.info("Starting sales-data-processor...")
    # TODO: Create a pipeline class that would coordinate all three layers
    spark_session = SparkSessionFactory.get_session()
    config = ProjectConfig()
    csv_extractor = CSVExtractor(
        spark_session, header=True, infer_schema=False, delimiter=","
    )
    parquet_extractor = ParquetExtractor(spark_session)
    delta_loader = DeltaLoader(spark_session, mode="overwrite", options=None)
    customer_processor = CustomerProcessor(columns=CustomerColumns())
    sales_processor = SalesProcessor(columns=SalesColumns())

    customer_df = parquet_extractor.extract(config.customer_source_path)
    customer_df = customer_processor.process(customer_df)
    delta_loader.load(customer_df, config.output_folder + "/customers")

    sales_df = csv_extractor.extract(config.sales_source_path)
    sales_df = sales_processor.process(sales_df)
    delta_loader.load(sales_df, config.output_folder + "/sales")


if __name__ == "__main__":
    main()
