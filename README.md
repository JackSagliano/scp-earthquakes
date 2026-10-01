# Earthquake co-occurrence analysis — Scala + Spark on Google Cloud Dataproc

Project for the *Scalable and Cloud Programming* course, University of Bologna, a.y. 2025-26.

Given a dataset of earthquakes (`longitude,latitude,date`), the program finds the **pair of distinct
locations that co-occur on the largest number of days** and lists those days in ascending order.
A location is the pair (latitude, longitude) rounded to the first decimal digit; two events co-occur
when they fall on the same day (UTC). Duplicated (location, day) events are counted once.

Output on the dataset provided with the assignment (full version):

```
((lat1, lon1), (lat2, lon2))
yyyy-mm-dd
...
```

## Approaches

The job is written with the RDD API following the map-reduce model. Three implementations of the same
analysis are provided (selected with `--approach`) and compared in the report:

| approach     | idea |
|--------------|------|
| `groupbykey` | baseline: `distinct` → `groupByKey(day)` → `flatMap(pairs)` → `reduceByKey` → max |
| `aggregate`  | one shuffle with map-side de-duplication (`aggregateByKey`), pairs encoded as a primitive `Long` |
| `pruning` *(default)* | exact algorithm that skips most pairs: two cells cannot co-occur more than `min(days(a), days(b))` times, so after a lower bound `L` is found among the most active cells only cells active on ≥ `L` days are paired |

All approaches return the same result. Ties (same number of co-occurrences) are broken
deterministically by choosing the smallest pair in (latitude, longitude) lexicographic order.

**Rounding**: coordinates are rounded to one decimal with *round half up* (ties away from zero:
`11.255 → 11.3`, `-122.85 → -122.9`) in exact decimal arithmetic (`BigDecimal`), not on `Double`, whose
binary representation would round values such as `37.35` down.

## Repository layout

```
build.sbt, project/          sbt build (Scala 2.12.18, Spark 3.5.3 "provided")
src/main/scala/earthquakes/  Main.scala (CLI, I/O, timing) · Analysis.scala (3 approaches) · Geo.scala (encodings)
data/sample-spec.csv         the example of the assignment, for a quick local test
data/sample-ties.csv         synthetic case with ties and duplicates (exercises every branch of `pruning`)
scripts/                     Dataproc automation (setup, clusters, jobs, benchmark, summary)
```

## Build

Requirements: JDK 11 or 17 and [sbt](https://www.scala-sbt.org/download).

```bash
sbt package
# -> target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar
```

## Run locally

```bash
sbt "run --input data/sample-spec.csv"
# or, with a local Spark installation:
spark-submit --master "local[*]" target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar \
  --input dataset-earthquakes-trimmed.csv --output out --approach pruning
```

Command-line arguments:

| argument | default | meaning |
|---|---|---|
| `--input <path>` | *(required)* | CSV file (local path or `gs://…`) with header `longitude,latitude,date` |
| `--output <path>` | none | output directory (overwritten); the result is always printed on stdout too |
| `--approach <name>` | `pruning` | `groupbykey`, `aggregate` or `pruning` |
| `--partitions <n>` | `spark.default.parallelism` | partitions used by `repartition` and by all the shuffles |
| `--topk <k>` | `64` | number of most active cells used to compute the lower bound (`pruning` only) |

Besides the result, the job prints a `PRUNING ...` line with the lower bound and the number of
candidate cells, and a `METRICS {...}` JSON line with the execution time and the
time of each phase.

## Run on Google Cloud Dataproc

All commands can be run from **Cloud Shell** (the `>_` icon in the Cloud console), which already has
`gcloud` installed and authenticated. From a local terminal, install the
[gcloud CLI](https://cloud.google.com/sdk/docs/install) and run `gcloud auth login` first.

Settings (project id, region, bucket name, image version) are in `scripts/config.sh`.

### 1. One-time setup

```bash
git clone https://github.com/JackSagliano/<repo-name>.git && cd <repo-name>
sbt package          # or upload a pre-built JAR

# enables the APIs, creates the bucket and uploads the JAR and the datasets
./scripts/setup.sh target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar \
                   dataset-earthquakes-full.csv dataset-earthquakes-trimmed.csv
```

Equivalent plain commands:

```bash
gcloud config set project <PROJECT_ID>
gcloud services enable dataproc.googleapis.com compute.googleapis.com
gcloud storage buckets create gs://<BUCKET> --location=europe-west1
gcloud storage cp target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar gs://<BUCKET>/jars/
gcloud storage cp dataset-earthquakes-full.csv gs://<BUCKET>/data/
```

### 2. Create a cluster

```bash
./scripts/create-cluster.sh 4        # 1 master + 4 workers; 0 = single node, 2, 3, 4
```

which runs:

```bash
gcloud dataproc clusters create scp-w4 --region=europe-west1 --image-version=2.2-debian12 \
  --num-workers 4 --master-boot-disk-size 240 --worker-boot-disk-size 240 \
  --master-machine-type=n2-standard-4 --worker-machine-type=n2-standard-4 --max-idle=30m
```

`--max-idle=30m` deletes the cluster automatically after 30 minutes without jobs, to protect the
education credits.

### 3. Submit the job

```bash
gcloud dataproc jobs submit spark --cluster=scp-w4 --region=europe-west1 \
  --jar=gs://<BUCKET>/jars/earthquake-cooccurrence_2.12-1.0.jar \
  -- --input gs://<BUCKET>/data/dataset-earthquakes-full.csv \
     --output gs://<BUCKET>/output/result --approach pruning --partitions 64

gcloud storage cat gs://<BUCKET>/output/result/part-00000     # the result
```

or `./scripts/run-job.sh <workers> <approach> <partitions>`, which also stores the metrics in
`results/metrics.jsonl`.

### 4. Delete the cluster

```bash
gcloud dataproc clusters delete scp-w4 --region=europe-west1
# or ./scripts/delete-cluster.sh 4
```

### Benchmark

`scripts/benchmark.sh <workers>` creates a cluster, runs the whole test matrix (three repetitions of
`aggregate` and `pruning`, several numbers of partitions, one run of `groupbykey`) and deletes the
cluster at the end, also on errors. The executors are fixed (2 executors × 2 cores × 5 GB per node,
no dynamic allocation) so that runs are comparable.

```bash
./scripts/benchmark.sh 0     # single node
./scripts/benchmark.sh 2
./scripts/benchmark.sh 3
./scripts/benchmark.sh 4
python3 scripts/summarize.py # -> results/summary.csv
```
