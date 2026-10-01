#!/usr/bin/env bash
# One-time setup: project, APIs, bucket, JAR and datasets upload.
# Usage: ./setup.sh [path/to/jar] [path/to/full.csv] [path/to/trimmed.csv]
set -euo pipefail
source "$(dirname "$0")/config.sh"

JAR="${1:-target/scala-2.12/${JAR_NAME}}"
FULL="${2:-}"
TRIMMED="${3:-}"

gcloud config set project "$PROJECT_ID"
gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com

if ! gcloud storage buckets describe "gs://${BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --location="$REGION" --uniform-bucket-level-access
fi

gcloud storage cp "$JAR" "$JAR_URI"
[ -n "$FULL" ]    && gcloud storage cp "$FULL" "$DATA_FULL"
[ -n "$TRIMMED" ] && gcloud storage cp "$TRIMMED" "$DATA_TRIMMED"
gcloud storage ls -l "gs://${BUCKET}/**"
