#!/usr/bin/env python3
"""Summarise results/metrics.jsonl into results/summary.csv (median of repetitions)."""
import csv, json, os, statistics, sys
from collections import defaultdict

here = os.path.dirname(os.path.abspath(__file__))
path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "..", "results", "metrics.jsonl")
runs = [json.loads(l) for l in open(path) if l.strip()]

groups = defaultdict(list)
for r in runs:
    groups[(r["dataset"], r["approach"], r["workers"], r["partitions"])].append(r)

rows = []
for (ds, ap, w, p), rs in sorted(groups.items()):
    secs = [r["seconds"] for r in rs]
    rows.append({"dataset": ds, "approach": ap, "workers": w, "partitions": p, "runs": len(rs),
                 "median_s": round(statistics.median(secs), 2), "min_s": min(secs), "max_s": max(secs),
                 "job_s": statistics.median(r["jobSeconds"] for r in rs),
                 "pair": rs[0]["pair"], "cooccurrences": rs[0]["cooccurrences"]})

out = os.path.join(os.path.dirname(path), "summary.csv")
with open(out, "w", newline="") as f:
    wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    wr.writeheader(); wr.writerows(rows)

print(f"{'dataset':8} {'approach':11} {'W':>2} {'P':>4} {'n':>2} {'median s':>9} {'min':>8} {'max':>8}")
for r in rows:
    print(f"{r['dataset']:8} {r['approach']:11} {r['workers']:>2} {r['partitions']:>4} {r['runs']:>2} "
          f"{r['median_s']:>9} {r['min_s']:>8} {r['max_s']:>8}")
pairs = {(r["pair"], r["cooccurrences"]) for r in runs if r["dataset"] == "full"}
print("\nresults on the full dataset:", pairs, "(consistent)" if len(pairs) <= 1 else "(INCONSISTENT!)")
print("written", out)
