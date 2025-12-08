[![ci](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml/badge.svg)](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/okdp/okdp-examples)](https://github.com/okdp/okdp-examples/releases/latest)
[![License Apache2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
<a href="https://okdp.io">
<img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>

A collection of hands-on examples, helper utilities, Jupyter notebooks, and data workflows showcasing how to work with the [OKDP Platform](https://okdp.io/).
This repository is meant to help you explore OKDP capabilities around compute, object storage, data catalog, SQL engines, Spark, and analytics.

# Notebooks

Jupyter notebooks that query Trino:

- Querying data using Trino (Python/SQLAlchemy)
- Querying data using Trino (SQL engine)

An index.ipynb notebook is also provided as an entry point.

# Superset

Use Apache Superset (SQL Lab) to query Trino and build visualizations/dashboards on top of the same datasets.

# Running the examples:

Using [okdp-ui](https://github.com/OKDP/okdp-sandbox), deploy the following components:

- Storage: [MinIO](https://www.min.io/)
- Data Catalog: [Hive Metastore](https://hive.apache.org/)
- Interactive Query: [Trino](https://trino.io/)
- Notebooks: [Jupyter](https://jupyter.org/)
- DataViz: [Apache Superset](https://superset.apache.org/)
- Applications: [okdp-examples](https://okdp.io)

# About the datasets

The Helm chart downloads public datasets at runtime, uploads them into object storage and creates appropriate Trino external tables.

> ℹ️ NOTE
>
> The datasets are not bundled in this repository or baked into container images.

