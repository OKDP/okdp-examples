# Airflow Examples

Apache Airflow DAGs and helpers showcasing how to orchestrate Spark jobs and
data workflows on the OKDP platform.

These DAGs are automatically pulled into the Airflow scheduler by the
`gitSync` sidecar configured in the
[okdp-sandbox Airflow package](https://github.com/OKDP/okdp-sandbox/blob/main/packages/okdp-packages/airflow/airflow.yaml)
(see `dagGitRepo` / `dagGitSubPath`). Any change pushed to `main` is reflected
in the scheduler within ~60 seconds.

## Available DAGs

| DAG | Description |
|---|---|
| `hello_world` | Minimal DAG, validates scheduler/worker connectivity |
| `hello_daily` | Same as above, scheduled daily |
| `spark_pi_example` | Submits the canonical Spark Pi job via `SparkApplication` |
| `orders_etl_daily` | Daily Spark ETL with dynamic ConfigMap-based script injection |
| `nyc_taxi_pipeline` | Reads NYC taxi data from S3, transforms with Spark, writes back |

## Running the NYC Taxi pipeline

The `nyc_taxi_pipeline` DAG requires a one-time setup (ConfigMap + S3 dataset):

```bash
# 1. Deploy the Spark ETL ConfigMap
./airflow/deploy_nyc_taxi.sh

# 2. Open the Airflow UI and trigger the DAG `nyc_taxi_pipeline`
open https://airflow.okdp.sandbox

# 3. Verify the results in SeaweedFS S3
kubectl run --rm -it s3-check --image=amazon/aws-cli:latest --restart=Never \
  --command -- aws --endpoint-url http://seaweedfs-pmj3xs-s3.default.svc.cluster.local:8333 \
  --no-verify-ssl s3 ls s3://okdp/examples/data/processed/nyc_taxi/ --recursive
```

## Architecture (NYC Taxi pipeline)

```
Airflow DAG (PythonOperator)
    → SparkApplication (Spark Operator)
        → Spark Driver + Executors
            → Read:  s3a://okdp/examples/data/raw/tripdata/yellow/  (11M+ rows)
            → Clean + Aggregate (168 rows: 24h × 7 days)
            → Write: s3a://okdp/examples/data/processed/nyc_taxi/yellow/run_id=.../nyc_taxi_aggregated.csv
```

## Datasets

NYC Yellow Taxi data is already provisioned in SeaweedFS by the
`okdp-examples` Helm chart at deployment time:

```
s3://okdp/examples/data/raw/tripdata/yellow/
├── month=2025-01/yellow_tripdata_2025-01.parquet  (59 MB)
├── month=2025-02/yellow_tripdata_2025-02.parquet  (60 MB)
└── month=2025-03/yellow_tripdata_2025-03.parquet  (70 MB)
```

No manual download required.

## Pipeline steps (NYC Taxi)

1. **Read** — 3 months of Parquet data from S3 (11M+ rows)
2. **Clean** — Filter invalid trips (fare ≤ 0, distance ≤ 0, etc.)
3. **Aggregate** — Group by hour and day-of-week (168 rows)
4. **Write** — Upload aggregated CSV to SeaweedFS via the JVM AWS SDK

> **Note**: writes use the JVM S3 SDK (not the Hadoop FileOutputCommitter)
> to work around a SeaweedFS `copyObject` quirk.

## Useful commands

```bash
# SparkApplication status
kubectl get sparkapplications -n default

# Spark driver logs
kubectl logs -n default -l spark-role=driver --tail=50

# List Airflow DAG runs
kubectl exec -n default deploy/airflow-main-scheduler -c scheduler -- \
  airflow dags list-runs -d nyc_taxi_pipeline -o plain
```

## Repository structure

```
airflow/
├── README.md
├── deploy_nyc_taxi.sh
├── dags/
│   ├── hello_world.py
│   ├── hello_daily.py
│   ├── spark_pi_example.py
│   ├── orders_etl_daily.py
│   ├── nyc_taxi_pipeline.py
│   └── spark_jobs/
│       └── orders_etl_job.py
├── manifests/
│   └── nyc-taxi-etl-configmap.yaml
└── tests/
    ├── test_dags.py
    └── run_integration_tests.sh
```

## License

Apache 2.0

---

**Built 🚀 for the OKDP Community**
<a href="https://okdp.io">
  <img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>
