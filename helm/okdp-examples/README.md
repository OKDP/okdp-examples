# okdp-examples

![Version: 1.0.0](https://img.shields.io/badge/Version-1.0.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.0.0](https://img.shields.io/badge/AppVersion-1.0.0-informational?style=flat-square)

A collection of hands-on examples, helper utilities, Jupyter notebooks, and data workflows
that showcases how to work with the OKDP Platform.

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| idirze | <idir.izitounene@kubotal.io> | <https://github.com/idirze> |

## Source Code

* <https://github.com/okdp/okdp-examples>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| commands.nyc_trip."03-nyc-tripdata-trino-external-tables.sql" | string | `"trino --server ${TRINO_SERVER_URL} --insecure <<SQL\n  CREATE SCHEMA IF NOT EXISTS lakehouse.nyc_tripdata;\n\n  CREATE TABLE IF NOT EXISTS lakehouse.nyc_tripdata.yellow (\n    vendorid INT,\n    tpep_pickup_datetime TIMESTAMP,\n    tpep_dropoff_datetime TIMESTAMP,\n    passenger_count INT,\n    trip_distance DOUBLE,\n    ratecodeid INT,\n    store_and_fwd_flag VARCHAR,\n    pulocationid INT,\n    dolocationid INT,\n    payment_type INT,\n    fare_amount DOUBLE,\n    extra DOUBLE,\n    mta_tax DOUBLE,\n    tip_amount DOUBLE,\n    tolls_amount DOUBLE,\n    improvement_surcharge DOUBLE,\n    total_amount DOUBLE,\n    congestion_surcharge DOUBLE,\n    airport_fee DOUBLE,\n    cbd_congestion_fee DOUBLE,\n    month VARCHAR\n  )\n  WITH (\n    external_location = 's3a://${BUCKET}/${BUCKET_PREFIX}/yellow/',\n    format = 'PARQUET',\n    partitioned_by = ARRAY['month']\n  );\n\n  CREATE TABLE IF NOT EXISTS lakehouse.nyc_tripdata.green (\n    vendorid INT,\n    lpep_pickup_datetime TIMESTAMP,\n    lpep_dropoff_datetime TIMESTAMP,\n    store_and_fwd_flag VARCHAR,\n    ratecodeid INT,\n    pulocationid INT,\n    dolocationid INT,\n    passenger_count INT,\n    trip_distance DOUBLE,\n    fare_amount DOUBLE,\n    extra DOUBLE,\n    mta_tax DOUBLE,\n    tip_amount DOUBLE,\n    tolls_amount DOUBLE,\n    improvement_surcharge DOUBLE,\n    total_amount DOUBLE,\n    payment_type INT,\n    trip_type INT,\n    congestion_surcharge DOUBLE,\n    cbd_congestion_fee DOUBLE,\n    month VARCHAR\n  )\n  WITH (\n    external_location = 's3a://${BUCKET}/${BUCKET_PREFIX}/green/',\n    format = 'PARQUET',\n    partitioned_by = ARRAY['month']\n  );\n\n  CREATE TABLE IF NOT EXISTS lakehouse.nyc_tripdata.fhv (\n    dispatching_base_num VARCHAR,\n    pickup_datetime TIMESTAMP,\n    dropoff_datetime TIMESTAMP,\n    pulocationid INT,\n    dolocationid INT,\n    sr_flag INT,\n    affiliated_base_number VARCHAR,\n    month VARCHAR\n  )\n  WITH (\n    external_location = 's3a://${BUCKET}/${BUCKET_PREFIX}/fhv/',\n    format = 'PARQUET',\n    partitioned_by = ARRAY['month']\n  );\nSQL\n"` |  |
| commands.nyc_trip.01-download-nyc-tripdata | string | `"mkdir -p /data/tripdata\ncd /data/tripdata;\nfor dataset in yellow green fhv; do\n  for month in 01 02 03; do\n    file=\"${dataset}_tripdata_2025-${month}.parquet\";\n    url=\"$DATA_URL/$file\";\n    echo \"→ $url\";\n    if curl -fsSLO \"$url\"; then\n      echo \"✅ Downloaded: $file\";\n    else\n      echo \"⚠️ Missing: $file\";\n    fi;\n  done;\ndone;\nls -lh /data/tripdata\necho \"✅ All downloads complete.\";\n"` |  |
| commands.nyc_trip.02-upload-nyc-tripdata-into-s3 | string | `"echo \"👉 Connect into S3\"\nmc alias set myminio $S3_ENDPOINT \"$S3_ACCESS_KEY\" \"$S3_SECRET_KEY\"\necho \"👉 MinIO alias configured successfully.\"\n### echo \"👉 Clean the examples folder if it exists\"\n### mc rm --recursive --force myminio/$BUCKET/examples\necho \"👉 Create the bucket if not exist\"\nmc mb myminio/$BUCKET || true\necho \"👉 Upload sample files into the correct folders\"\n\nfor category in yellow green fhv; do\n  echo \"🚀 Processing category: $category\"\n  for file in /data/tripdata/${category}_*.parquet; do\n    [ -e \"$file\" ] || continue  # skip if no files match\n    month=$(basename \"$file\" | sed -E 's/.*_([0-9]{4}-[0-9]{2})\\.parquet/\\1/')\n    echo \"📦 Uploading $file → ${BUCKET_PREFIX}/$category/month=$month/\"\n    mc cp \"$file\" \"myminio/$BUCKET/${BUCKET_PREFIX}/$category/month=$month/\"\n  done\ndone\n\necho \"👉 Verify bucket structure\"\nmc tree myminio/$BUCKET\nmc ls --recursive myminio/$BUCKET\n"` |  |
| commands.nyc_trip.04-nyc-tripdata-trino-synchronize-partitions | string | `"trino --server ${TRINO_SERVER_URL} --insecure <<SQL\n  CALL lakehouse.system.sync_partition_metadata(\n    schema_name => 'nyc_tripdata',\n    table_name => 'yellow',\n    mode => 'ADD'\n  );\n  CALL lakehouse.system.sync_partition_metadata(\n    schema_name => 'nyc_tripdata',\n    table_name => 'green',\n    mode => 'ADD'\n  );\n  CALL lakehouse.system.sync_partition_metadata(\n    schema_name => 'nyc_tripdata',\n    table_name => 'fhv',\n    mode => 'ADD'\n  );\nSQL\n"` |  |
| commands.nyc_trip.05-nyc-tripdata-trino-validate | string | `"trino --server ${TRINO_SERVER_URL} --insecure <<SQL\n  SHOW SCHEMAS FROM lakehouse;\n\n  SHOW TABLES FROM lakehouse.nyc_tripdata;\n\n  DESCRIBE lakehouse.nyc_tripdata.yellow;\n  DESCRIBE lakehouse.nyc_tripdata.green;\n  DESCRIBE lakehouse.nyc_tripdata.fhv;\n\n  SELECT *\n  FROM lakehouse.nyc_tripdata.yellow\n  LIMIT 10;\nSQL\n"` |  |
| extraEnvRaw | list | `[]` | Extra environment variables in RAW format that will be passed into pods |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"Always"` |  |
| image.repository | string | `"quay.io/okdp/okdp-examples"` |  |
| image.tag | string | `"latest"` |  |
| imagePullSecrets | list | `[]` |  |
| job.annotations."helm.sh/hook" | string | `"post-install,post-upgrade"` |  |
| job.annotations."helm.sh/hook-delete-policy" | string | `"before-hook-creation"` |  |
| job.backoffLimit | int | `2` |  |
| job.restartPolicy | string | `"Never"` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podAnnotations | object | `{}` |  |
| podLabels | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| resources | object | `{}` |  |
| securityContext | object | `{}` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.automount | bool | `true` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| tolerations | list | `[]` |  |
| volumeMounts | list | `[]` |  |
| volumes | list | `[]` |  |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.13.1](https://github.com/norwoodj/helm-docs/releases/v1.13.1)
