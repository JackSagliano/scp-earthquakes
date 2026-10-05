# Co-occorrenza di eventi sismici — Scala + Spark su Dataproc

Progetto per il corso Scalable and Cloud Programming, Università di Bologna, a.a. 2025-26.

Il programma legge un CSV di terremoti (`longitude,latitude,date`) e trova le due località distinte
(coppia lat/lon arrotondata alla prima cifra decimale) con il maggior numero di giorni di co-occorrenza
sismica, elencando quelle date in ordine. Tre implementazioni map-reduce su RDD — `groupbykey`,
`aggregate` e `pruning` (default) — descritte e confrontate, con l'analisi di scalabilità e prestazioni,
nel report.

## Build

Servono JDK 11/17 e sbt.

```
sbt package      # -> target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar
```

## Esecuzione in locale

Il modo più semplice, senza installare Spark a parte, è `sbt run`: il `build.sbt` include Spark nel
classpath durante l'esecuzione locale. (Le virgolette raggruppano gli argomenti per il programma.)

```
sbt "run --input data/sample-spec.csv --approach pruning"
sbt "run --input dataset-earthquakes-trimmed.csv --approach pruning"
```

In alternativa, con un'installazione locale di Spark, si può usare `spark-submit` sul jar di `sbt package`:

```
spark-submit --master "local[*]" target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar --input dataset-earthquakes-trimmed.csv --output out --approach pruning
```

| argomento | default | significato |
|---|---|---|
| `--input <path>` | obbligatorio | CSV (locale o `gs://…`), header `longitude,latitude,date` |
| `--output <path>` | nessuno | directory di output (sovrascritta); il risultato va comunque su stdout |
| `--approach <name>` | `pruning` | `groupbykey`, `aggregate`, `pruning` |
| `--partitions <n>` | = core | partizioni per `repartition` e tutti gli shuffle |
| `--topk <k>` | `64` | celle più attive usate per il limite inferiore (solo pruning) |
| `--rounding <mode>` | `half-up` | `half-up`, `half-even`, `math-round` |

## Esecuzione su Dataproc

Dalla Cloud Shell (gcloud già pronto), oppure da gcloud locale dopo `gcloud auth login`.

```
# 1. progetto, API e bucket
gcloud config set project [PROJECT_ID]
gcloud services enable dataproc.googleapis.com
gcloud storage buckets create gs://[BUCKET] --location=europe-west1

# 2. carica il jar e i dataset
gcloud storage cp target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar gs://[BUCKET]/jars/
gcloud storage cp dataset-earthquakes-full.csv gs://[BUCKET]/data/

# 3. crea il cluster (n2-standard-4 obbligatorio; disco da 240 GB per non sforare i crediti)
#    single node:
gcloud dataproc clusters create [CLUSTER] --region=europe-west1 --single-node \
  --master-boot-disk-size 240 --master-machine-type=n2-standard-4
#    con N worker:
gcloud dataproc clusters create [CLUSTER] --region=europe-west1 --num-workers=[N] \
  --master-boot-disk-size 240 --worker-boot-disk-size 240 \
  --master-machine-type=n2-standard-4 --worker-machine-type=n2-standard-4

# 4. sottometti il job
gcloud dataproc jobs submit spark --cluster=[CLUSTER] --region=europe-west1 \
  --jar=gs://[BUCKET]/jars/earthquake-cooccurrence_2.12-1.0.jar \
  -- --input gs://[BUCKET]/data/dataset-earthquakes-full.csv \
     --output gs://[BUCKET]/output/result --approach pruning --partitions 64
gcloud storage cat gs://[BUCKET]/output/result/part-00000

# 5. elimina il cluster a fine lavoro
gcloud dataproc clusters delete [CLUSTER] --region=europe-west1
```

Gli script in `scripts/` automatizzano gli stessi passaggi (setup, creazione cluster, job, benchmark ed
eliminazione) e salvano le misure in `results/`; il dettaglio delle prove è nel report.
