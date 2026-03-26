[![ci](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml/badge.svg)](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/okdp/okdp-examples)](https://github.com/okdp/okdp-examples/releases/latest)
[![License Apache2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
<a href="https://okdp.io">
<img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>

A collection of hands-on examples, helper utilities, Jupyter notebooks, and data workflows showcasing how to work with the [OKDP Platform](https://okdp.io/).
This repository is meant to help you explore OKDP capabilities around compute, object storage, data catalog, Open Table Formats, SQL engines, Spark, and analytics.

The project follows a [**Bronze → Silver → Gold** Medallion](https://www.databricks.com/blog/what-is-medallion-architecture) architecture:

- **Bronze** stores raw Parquet files in S3-compatible object storage and supports exploration, profiling, and source understanding.
- **Silver** publishes a trusted, conformed Iceberg table in the `silver` Polaris catalog.
- **Gold** publishes curated business-facing Iceberg tables in the `gold` Polaris catalog.

Over time, these examples will be extended with features, such as:

- Shared metadata with stronger schema enforcement and evolution.
- Snapshot-based table management (time travel, retention, cleanup).
- Incremental processing and analytics-ready datasets, etc.
- Automated ingestion, transformations, and dataset publishing through Apache Airflow.

```text
                                       +-----------+
                                       | Keycloak  |
                                       |  OIDC/IdP |
                                       +-----+-----+
                                             ^
                                             | OIDC / OAuth2
                                             |
        +------+     +----------+      +-----+-----+       +-----------+
        | User |---->| Superset |----->|   Trino   |------>|   Bronze  |
        +--+---+     +-----+----+      +-----+-----+       |HMS ext tbl|
           |               |                   |           +-----+-----+
           |               | SQL over HTTPS    | Hive            |
           |               |                   v MS              |
           |         +-----+-----+        +---------+            |
           |         | SQLAlchemy |------>| Hive MS |            |
           |         +-----------+        +---------+            |
           |                                                    S3
           |                                                     |
           |                                                     v
           |         +-------------+      REST + OAuth2    +-----+-----+
           +-------->|   Jupyter   |---------------------->|  Polaris  |
                     | PySpark/notb|<----------------------| REST cat  |
                     +------+------+   catalog + temp creds +-----+----+
                            |                                        |
                            | direct S3 with temp creds              | STS AssumeRole
                            | for Silver / Gold writes               | + role policy
                            v                                        v
                       +----+----------------------------------------+----+
                       |                 SeaweedFS S3 + IAM + STS         |
                       +----+-------------------------------+-------------+
                            ^                               ^
                            | static S3 creds               | temp S3 creds
                            |                               |
                       +----+-----+                    +----+-----+
                       |  Bronze  |                    | Silver   |
                       | raw pq   |                    | Iceberg  |
                       +----------+                    +----+-----+
                                                            |
                                                            v
                                                       +----+-----+
                                                       |   Gold   |
                                                       | Iceberg  |
                                                       +----------+
```

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
- Data Catalog: [Hive Metastore](https://hive.apache.org/), [Apache Polaris](https://polaris.apache.org/)
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

# Know issues
1. [Polaris - Spark Iceberg REST Catalog refresh token](https://github.com/apache/iceberg/issues/12363)
    > Long-running jobs may need more metadata calls to Polaris during execution, not just one initial call
2. [Trino - Issue with Vended Credential Renewal with Iceberg REST Catalog](https://github.com/trinodb/trino/issues/25827)
   > Reported upstream: with `iceberg.rest-catalog.vended-credentials-enabled=true`, long-running queries may fail once the STS token expires because Trino appears not to refresh vended credentials from the Iceberg REST catalog `/credentials` endpoint.
   >
   > A fix has been proposed in [PR #28792](https://github.com/trinodb/trino/pull/28792), but it is still under review, so this behavior should be validated in our environment.
3. [Trino - Extra credential support for user token passthrough](https://github.com/trinodb/trino/issues/27197)
    > Requests support for passing per-user OAuth tokens/credentials to the Iceberg REST catalog
4. [Trino - Include oauth user in the request to the iceberg REST catalog](https://github.com/trinodb/trino/issues/26320)
   > [Starburst supports OAuth 2.0 token pass-through for the Iceberg REST catalog](https://docs.starburst.io/latest/object-storage/metastores.html#oauth-2-0-token-pass-through), which forwards the delegated OAuth token from the coordinator to the catalog:
   >
   > ```properties
   > http-server.authentication.type=DELEGATED-OAUTH2
   > iceberg.rest-catalog.security=OAUTH2_PASSTHROUGH
   > ```
5. [STS assume role fails with credentials (from Lakekeeper) due to incomplete STS implementation](https://github.com/seaweedfs/seaweedfs/discussions/8312)
   > The discussion initially points to a possible SeaweedFS STS compatibility issue, but the later reproducer narrows the failure to Lakekeeper's scoped session policy: multipart writes fail when the policy omits the required multipart S3 permissions.
   >
   > It demonstrates that multipart upload can fail if the scoped session policy does not include multipart actions such as:
   > - `s3:CreateMultipartUpload`
   > - `s3:UploadPart`
   > - `s3:CompleteMultipartUpload`
   > - `s3:AbortMultipartUpload`
   >
   > The issue seems to be fixed by the pr [#8445](https://github.com/seaweedfs/seaweedfs/pull/8445).

