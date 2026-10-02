#!/usr/bin/env bash
# Usage: ./create-cluster.sh <workers>     (0 = single-node cluster, master only)
set -euo pipefail
source "$(dirname "$0")/config.sh"
W="${1:?number of workers (0, 2, 3, 4)}"
NAME="$(cluster_name "$W")"

if gcloud dataproc clusters describe "$NAME" --project "$PROJECT_ID" --region "$REGION" >/dev/null 2>&1; then
  echo ">> cluster $NAME already exists, reusing it"
  exit 0
fi

if [ "$W" -eq 0 ]; then
  SHAPE=(--single-node)
else
  SHAPE=(--num-workers "$W" --worker-machine-type "$MACHINE_TYPE" --worker-boot-disk-size "$DISK_SIZE")
fi

gcloud dataproc clusters create "$NAME" \
  --project "$PROJECT_ID" --region "$REGION" \
  --image-version "$IMAGE_VERSION" \
  --master-machine-type "$MACHINE_TYPE" --master-boot-disk-size "$DISK_SIZE" \
  "${SHAPE[@]}" \
  --max-idle "$MAX_IDLE"
