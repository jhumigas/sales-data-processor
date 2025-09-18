from pydantic import Field

from pydantic_settings import BaseSettings, SettingsConfigDict


class ProjectConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    env: str = Field(default="dev", alias="ENV")
    spark_app_name: str = Field(default="sales-data-processor", alias="SPARK_APP_NAME")
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_host: str = Field(default="localhost", alias="SPARK_HOST")
    spark_bind_address: str = Field(default="0.0.0.0", alias="SPARK_BIND_ADDRESS")

    customer_source_path: str = Field(alias="CUSTOMER_SOURCE_PATH")
    sales_source_path: str = Field(alias="SALES_SOURCE_PATH")
    output_folder: str = Field(alias="OUTPUT_FOLDER")
