# Co-occorrenza di eventi sismici — Scala + Spark su Dataproc

Progetto per il corso di Scalable and Cloud Programming, Università di Bologna, a.a. 2025-26.

Legge un CSV di terremoti (`longitude,latitude,date`) e trova le due località distinte che hanno un
terremoto nello stesso giorno il maggior numero di volte, elencando quei giorni in ordine. Una località
è la coppia (lat, lon) arrotondata alla prima cifra decimale (~11 km); "stesso giorno" significa lo
stesso giorno di calendario in UTC. Più terremoti nella stessa cella lo stesso giorno collassano in un
unico evento (cella, giorno), altrimenti la coppia vincente sarebbe semplicemente una cella con se stessa.

L'analisi è implementata tre volte con le API RDD (modello map-reduce) e confrontata nel report:

- `groupbykey` — baseline: `distinct` → raggruppa le celle per giorno → emette le coppie →
  `reduceByKey` → massimo.
- `aggregate` — i gruppi giornalieri in un unico `aggregateByKey` (deduplica lato map), coppie
  impacchettate in un `Long`.
- `pruning` (default) — esatto e molto più veloce: due celle co-occorrono al massimo
  `min(days(a), days(b))` volte, quindi, trovato un limite inferiore `L`, si accoppiano solo le celle
  attive in almeno `L` giorni. Sul dataset completo restano 2 celle candidate su 213.062, cioè ~2,5M
  di coppie invece di ~223M.

Tutti e tre restituiscono la stessa coppia (i pareggi si risolvono con la coppia minore in ordine
(lat, lon)). Le coordinate sono arrotondate con `BigDecimal` sulla stringa, non su un `Double`;
`--rounding` sceglie half-up (default), half-even o math-round — la coppia è identica in tutti e tre i
casi, cambia solo il numero di giorni (10.014 contro 10.032).

## Risultato

Dataset completo (3.445.751 eventi, 1990–2023):

```
((38.8, -122.8), (38.8, -122.7))
1990-01-05
...
2023-07-29
```

10.014 giorni di co-occorrenza (3.485 sul dataset ridotto) — il campo geotermico di The Geysers,
California. Verificato in modo indipendente con uno script Python (`results/local/`).

Tempi mediani, dataset completo (s, 4 partizioni/core; tutte le esecuzioni in `results/metrics.jsonl`):

| cluster | core | groupbykey | aggregate | pruning |
|---|---:|---:|---:|---:|
| single node | 4 | 910 | 805 | 49 |
| 2 worker | 8 | 323 | 305 | 35 |
| 3 worker | 12 | 213 | 215 | 37 |
| 4 worker | 16 | 151 | 152 | 31 |

## Build ed esecuzione

Servono JDK 11/17 e sbt.

```bash
sbt package        # -> target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar

# in locale
spark-submit --master "local[*]" target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar \
  --input dataset-earthquakes-trimmed.csv --output out --approach pruning
```

| argomento | default | significato |
|---|---|---|
| `--input <path>` | obbligatorio | CSV (locale o `gs://…`), header `longitude,latitude,date` |
| `--output <path>` | nessuno | directory di output (sovrascritta); il risultato va comunque su stdout |
| `--approach <name>` | `pruning` | `groupbykey`, `aggregate`, `pruning` |
| `--partitions <n>` | = core | partizioni per `repartition` e tutti gli shuffle |
| `--topk <k>` | `64` | celle più attive usate per il limite inferiore (solo pruning) |
| `--rounding <mode>` | `half-up` | `half-up`, `half-even`, `math-round` |

## Dataproc

Da Cloud Shell (gcloud già pronto), o da gcloud locale dopo `gcloud auth login`. Configurazione in
`scripts/config.sh`.

```bash
git clone https://github.com/JackSagliano/scp-earthquakes.git && cd scp-earthquakes
sbt package
./scripts/setup.sh target/scala-2.12/earthquake-cooccurrence_2.12-1.0.jar \
                   dataset-earthquakes-full.csv dataset-earthquakes-trimmed.csv   # API, bucket, upload
./scripts/create-cluster.sh 4     # 0 = single node; altrimenti N worker
```

`create-cluster.sh` esegue la configurazione richiesta (disco da `240` GB e `n2-standard-4` come da
specifica; `--max-idle=30m` è una mia aggiunta, così un cluster che mi dimentico si cancella da solo
prima di consumare i crediti):

```bash
gcloud dataproc clusters create scp-w4 --region=europe-west1 --image-version=2.2-debian12 \
  --num-workers 4 --master-boot-disk-size 240 --worker-boot-disk-size 240 \
  --master-machine-type=n2-standard-4 --worker-machine-type=n2-standard-4 --max-idle=30m
```

```bash
# sottomette un job
gcloud dataproc jobs submit spark --cluster=scp-w4 --region=europe-west1 \
  --jar=gs://<BUCKET>/jars/earthquake-cooccurrence_2.12-1.0.jar \
  -- --input gs://<BUCKET>/data/dataset-earthquakes-full.csv \
     --output gs://<BUCKET>/output/result --approach pruning --partitions 64
gcloud storage cat gs://<BUCKET>/output/result/part-00000

# oppure ./scripts/run-job.sh <workers> <approach> <partitions>   (registra anche su results/metrics.jsonl)
gcloud dataproc clusters delete scp-w4 --region=europe-west1   # a fine lavoro
```

## Benchmark

`scripts/benchmark.sh <workers>` crea un cluster, esegue l'intera matrice di prove (3× `aggregate` e
`pruning`, una scansione sul numero di partizioni, una esecuzione di `groupbykey`) e lo cancella alla
fine, anche in caso di errore. Gli executor sono fissati (2 × 2 core × 3 GB per nodo, driver da 2 GB,
niente allocazione dinamica) per rendere confrontabili le esecuzioni. Nota: sul single node ho dovuto
abbassare gli executor a 3 GB — a 5 GB il driver veniva continuamente terminato, con driver, executor e
servizi Hadoop che insieme superavano i 16 GB della macchina.

```bash
./scripts/benchmark.sh 0   # poi 2, 3, 4
python3 scripts/summarize.py   # -> results/summary.csv
```

## Struttura

```
src/main/scala/earthquakes/   Main.scala (CLI, I/O, tempi) · Analysis.scala (3 approcci) · Geo.scala (codifiche)
data/sample-spec.csv          esempio della specifica  ·  data/sample-ties.csv  pareggi + duplicati (copre pruning)
scripts/                      automazione Dataproc  ·  results/  metriche e riepilogo
```

Nota: nell'esempio della specifica il punto `38.147, 13.324` si arrotonda a `(38.1, 13.3)`, non a
`(38.1, 13.4)`, quindi il programma riporta 2 giorni di co-occorrenza invece di 3 — segue la regola di
arrotondamento della specifica; è l'esempio svolto a essere incoerente con essa.
