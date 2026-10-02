#!/usr/bin/env bash
# Submits one job and appends its METRICS line to results/metrics.jsonl
# Usage: ./run-job.sh <workers> <approach> <partitions> [dataset: full|trimmed] [repetition]
set -euo pipefail
source "$(dirname "$0")/config.sh"
W="${1:?workers}"; APPROACH="${2:?groupbykey|aggregate|pruning}"; P="${3:?partitions}"
DATASET="${4:-full}"; REP="${5:-1}"
NAME="$(cluster_name "$W")"
INPUT="$DATA_FULL"; [ "$DATASET" = "trimmed" ] && INPUT="$DATA_TRIMMED"
OUT="gs://${BUCKET}/output/${NAME}-${APPROACH}-p${P}-${DATASET}"

# Fixed, identical executors on every cluster (no dynamic allocation) so that
# runs are comparable: 2 executors x 2 cores x 3 GB per node, driver 2 GB.
# The memory is sized for the single-node cluster, where driver, executors and
# all the Hadoop daemons share the 16 GB of one n2-standard-4: with larger
# executors Dataproc kills the driver because of memory pressure on the master.
NODES=$(( W == 0 ? 1 : W ))
PROPS="spark.dynamicAllocation.enabled=false,spark.executor.instances=$((2 * NODES)),spark.executor.cores=2,spark.executor.memory=3g,spark.driver.memory=2g"

mkdir -p "$RESULTS_DIR"
LOG="$RESULTS_DIR/logs/${NAME}-${APPROACH}-p${P}-${DATASET}-r${REP}.log"
mkdir -p "$(dirname "$LOG")"

START=$(date +%s)
gcloud dataproc jobs submit spark \
  --project "$PROJECT_ID" --region "$REGION" --cluster "$NAME" \
  --class earthquakes.Main --jars "$JAR_URI" \
  --properties "$PROPS" \
  -- --input "$INPUT" --output "$OUT" --approach "$APPROACH" --partitions "$P" \
  2>&1 | tee "$LOG"
END=$(date +%s)

METRICS=$(grep -o 'METRICS {.*}' "$LOG" | tail -1 | sed 's/^METRICS //')
if [ -n "$METRICS" ]; then
  # add cluster info and the end-to-end job time measured from the client
  echo "$METRICS" | sed "s/^{/{\"workers\":$W,\"dataset\":\"$DATASET\",\"rep\":$REP,\"jobSeconds\":$((END - START)),/" \
    >> "$RESULTS_DIR/metrics.jsonl"
  echo ">> saved: $(tail -1 "$RESULTS_DIR/metrics.jsonl")"
else
  echo ">> job failed, see $LOG" >&2
  exit 1
fi
