# README

## Overview

Simple python application, extracting, processing and loading customer and sales data.
In this case, we support that the ETL pipeline is handling data from bronze to silver layer, hence not much business logic is implemented.
We only limit the transformation to applying schema, handling missing values etc.

For the CSV data, we could have had an intermediate storage format into parquet, but for sake of simplicity we directly write everything into the delta lake

## Assumptions

The supposed data flow is the following:

bronze layer (Object Storage) -> etl pipeline -> silver layer (delta lake)

Data Storage:

* Support only reading from local filesystem
* Write data locally

## Pre-requisities

Make sure you have:

- [uv](https://docs.astral.sh/uv/): Python package and project manager ([instructions](https://docs.astral.sh/uv/getting-started/installation/))
- [Docker](https://www.docker.com/get-started/) or a docker container manager (use [colima](https://github.com/abiosoft/colima#installation) for macOS)
- [sdkman](https://sdkman.io/) to manage SDKs
- [Java](https://adoptium.net/fr/temurin/releases?version=11), you can use `sdk install java 11.0.28-tem`
- [Spark](https://spark.apache.org/releases/spark-release-3-5-0.html) installed, you can use `sdk install spark 3.5.3`


## Setup local env

Setup a python virtual environments and install dependencies with the following

```sh
uv sync
```

You can then run the pipeline with 

```sh
python src/sales_data_processor/main.py
```

## Run tests

For unit tests

```sh
make unit-tests # Check if unit tests work
```

### Project structure

```text
├── example.env                  <-- Sample for local environment variables
├── pyproject.toml
├── uv.lock
├── Makefile                     <-- Commands to build, test and run locally the project
├── .pre-commit-config.yaml
├── .python-version
├── data
├── notebooks                    <-- Simple notebook used to explore the data
├── pyproject.toml
├── README.md
├── src                          
│   └── sales_data_processor
│       ├── __init__.py
│       ├── config.py                <-- Application configuration module
│       ├── core                     <-- Core utilities of the application where we handle spark_session
│       ├── extractors               <-- Data extractor for csv, parquet
│       ├── processors               <-- Data processors for sales and customers data
│       ├── loaders                  <-- Data loaders, for delta lake tables
│       ├── logger.py
│       └── main.py                  <-- Main script to run the application

└── tests

```

## References

* [Local Setup With Spark and Docker](https://medium.com/programmers-journey/deadsimple-pyspark-docker-spark-cluster-on-your-laptop-9f12e915ecf4)