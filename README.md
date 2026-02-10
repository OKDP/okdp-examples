[![ci](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml/badge.svg)](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/okdp/okdp-examples)](https://github.com/okdp/okdp-examples/releases/latest)
[![License Apache2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
<a href="https://okdp.io">
<img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>

A collection of hands-on examples, helper utilities, Jupyter notebooks, and data workflows showcasing how to work with the [OKDP Platform](https://okdp.io/).
This repository is meant to help you explore OKDP capabilities around compute, object storage, data catalog, SQL engines, Spark, and analytics.

Over time, these examples will be extended with lakehouse-oriented features, such as:

- Open table formats (e.g. Apache Iceberg and/or Delta Lake).
- Shared metadata with stronger schema enforcement and evolution.
- Snapshot-based table management (time travel, retention, cleanup).
- Incremental processing and analytics-ready datasets, etc.

# Notebooks

The notebooks analyze datasets stored as Parquet on S3-compatible storage (MinIO).
The same underlying dataset is queried using Trino and Spark.

An [index.ipynb](./notebooks/index.ipynb) notebook is also provided as an entry point.

## Trino notebooks

The following notebooks query data using Trino:

- Querying data using Trino (Python/SQLAlchemy).
- Querying data using Trino (SQL engine).

These notebooks use Trino external tables defined over Parquet data stored in object storage and registered via a metadata service.

## PySpark notebook

A PySpark notebook is included to showcase Spark-native exploratory data analysis on the same dataset.

# Superset

Use Apache Superset (SQL Lab) to query Trino and build visualizations/dashboards on top of the same datasets.

# Running the examples:

Using [okdp-ui](https://github.com/OKDP/okdp-sandbox), deploy the following components:

- Storage: [SeaweedFS](https://github.com/seaweedfs/seaweedfs)
- Data Catalog: [Hive Metastore](https://hive.apache.org/)
- Interactive Query: [Trino](https://trino.io/)
- Notebooks: [Jupyter](https://jupyter.org/)
- DataViz: [Apache Superset](https://superset.apache.org/)
- Applications: [okdp-examples](https://okdp.io)

# About the datasets

At deployment time, the Helm chart:
1. Downloads public datasets.
2. Uploads them into object storage.
3. Creates the corresponding Trino external tables.

> ℹ️ NOTE
>
> The datasets are not bundled in this repository and are not baked into container images.

