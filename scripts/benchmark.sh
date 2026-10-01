#!/usr/bin/env bash
# Full benchmark on ONE cluster size: creates the cluster, runs the whole test
# matrix, and ALWAYS deletes the cluster at the end (even on errors / Ctrl-C).
#
# Usage: ./benchmark.sh <workers>          e.g.  ./benchmark.sh 0 ; ./benchmark.sh 2 ; ...
# Tunables (env vars): REPS (default 3), DATASET (full), SKIP_GROUPBYKEY=1
set -uo pipefail
DIR="$(dirname "$0")"
source "$DIR/config.sh"
W="${1:?number of workers (0 = single node, 2, 3, 4)}"
REPS="${REPS:-3}"
DATASET="${DATASET:-full}"

NODES=$(( W == 0 ? 1 : W ))
CORES=$(( 4 * NODES ))                  # executor cores available to the job

cleanup() { echo ">> deleting cluster $(cluster_name "$W")"; "$DIR/delete-cluster.sh" "$W" || true; }
trap cleanup EXIT

"$DIR/create-cluster.sh" "$W" || exit 1

run() { "$DIR/run-job.sh" "$W" "$1" "$2" "$DATASET" "$3" || echo ">> FAILED: $*"; }

# A) scalability: both main approaches, 4 partitions per core, REPS repetitions
for r in $(seq 1 "$REPS"); do
  run aggregate $((4 * CORES)) "$r"
  run pruning   $((4 * CORES)) "$r"
done

# B) effect of the number of partitions (1 repetition each)
for f in 1 2 8; do
  run aggregate $((f * CORES)) 1
  run pruning   $((f * CORES)) 1
done

# C) baseline groupByKey approach (1 repetition)
[ "${SKIP_GROUPBYKEY:-0}" = "1" ] || run groupbykey $((4 * CORES)) 1

echo ">> done: results in $RESULTS_DIR/metrics.jsonl"
