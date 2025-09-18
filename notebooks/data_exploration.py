import marimo

__generated_with = "0.15.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    input_folder = "./data"
    customers_filename = "customer_data.parquet"
    sales_filename = "sales_data.csv"
    return customers_filename, input_folder, pl, sales_filename


@app.cell
def _(input_folder, pl, sales_filename):
    sale_df = pl.read_csv(f"{input_folder}/{sales_filename}")
    return


@app.cell
def _(sales_df):
    sales_df
    return


@app.cell
def _():
    # customers_df = pl.read_parquet(f"{input_folder}/{customers_filename}/*.parquet")
    return


@app.cell
def _():
    # customers_df
    return


@app.cell
def _():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("DataExploSparkApp")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    return (spark,)


@app.cell
def _(customers_filename, input_folder, spark):
    customer_spark_df = spark.read.parquet(f"{input_folder}/{customers_filename}")
    return (customer_spark_df,)


@app.cell
def _(customer_spark_df):
    customer_spark_df.show()
    return


@app.cell
def _(input_folder, sales_filename, spark):
    sales_spark_df = spark.read.csv(f"{input_folder}/{sales_filename}")
    return (sales_spark_df,)


@app.cell
def _(sales_spark_df):
    sales_spark_df.show()
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
