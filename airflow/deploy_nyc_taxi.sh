#!/bin/bash
#
# NYC Taxi pipeline deployment (Airflow + Spark Operator).
# Usage: ./airflow/deploy_nyc_taxi.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

NAMESPACE="default"

echo "======================================================================="
echo "NYC Taxi Pipeline - Deployment"
echo "======================================================================="

# --- Prerequisites ---
echo ""
echo "1. Checking prerequisites"
echo "-----------------------------------------------------------------------"

for check in \
  "kubectl:command -v kubectl" \
  "Cluster:kubectl cluster-info" \
  "Spark Operator:kubectl get crd sparkapplications.sparkoperator.k8s.io" \
  "ServiceAccount spark:kubectl get sa spark -n $NAMESPACE" \
  "S3 Secret:kubectl get secret creds-examples-s3 -n $NAMESPACE"; do
    label="${check%%:*}"
    cmd="${check#*:}"
    if eval "$cmd" &>/dev/null; then
        echo -e "${GREEN}✓ $label${NC}"
    else
        echo -e "${RED}✗ $label${NC}"
        exit 1
    fi
done

# --- ConfigMap ---
echo ""
echo "2. Deploying the Spark ETL ConfigMap"
echo "-----------------------------------------------------------------------"

if kubectl apply -f "$SCRIPT_DIR/manifests/nyc-taxi-etl-configmap.yaml"; then
    echo -e "${GREEN}✓ ConfigMap deployed${NC}"
else
    echo -e "${RED}✗ ConfigMap deployment failed${NC}"
    exit 1
fi

# --- Done ---
echo ""
echo "======================================================================="
echo -e "${GREEN}Deployment complete${NC}"
echo "======================================================================="
echo ""
echo "DAGs are pulled into Airflow automatically by the gitSync sidecar."
echo ""
echo "Next steps:"
echo "  1. Open the Airflow UI: https://airflow.okdp.sandbox"
echo "  2. Trigger the DAG 'nyc_taxi_pipeline'"
echo "  3. Monitor: kubectl get sparkapplication -n $NAMESPACE -w"
echo ""
