#!/usr/bin/env bash
# Usage: ./delete-cluster.sh <workers>
set -euo pipefail
source "$(dirname "$0")/config.sh"
W="${1:?number of workers (0, 2, 3, 4)}"
gcloud dataproc clusters delete "$(cluster_name "$W")" --project "$PROJECT_ID" --region "$REGION" --quiet
