#!/usr/bin/env bash
# Shared settings for all the scripts. Edit PROJECT_ID / BUCKET if needed.
PROJECT_ID="${PROJECT_ID:-scalable-project-510219}"
REGION="${REGION:-europe-west1}"
BUCKET="${BUCKET:-${PROJECT_ID}-earthquakes}"       # bucket names are global: prefix with the project id
IMAGE_VERSION="${IMAGE_VERSION:-2.2-debian12}"       # Spark 3.5, Scala 2.12, Java 11
MACHINE_TYPE="n2-standard-4"                         # mandatory for the project
DISK_SIZE=240
MAX_IDLE="${MAX_IDLE:-30m}"                          # safety net: idle clusters delete themselves

JAR_NAME="earthquake-cooccurrence_2.12-1.0.jar"
JAR_URI="gs://${BUCKET}/jars/${JAR_NAME}"
DATA_FULL="gs://${BUCKET}/data/dataset-earthquakes-full.csv"
DATA_TRIMMED="gs://${BUCKET}/data/dataset-earthquakes-trimmed.csv"
RESULTS_DIR="${RESULTS_DIR:-$(dirname "${BASH_SOURCE[0]}")/../results}"

# cluster name from the number of workers (0 = single node)
cluster_name() { if [ "$1" -eq 0 ]; then echo "scp-single"; else echo "scp-w$1"; fi; }
