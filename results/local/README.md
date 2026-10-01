Local runs on the trimmed dataset (1,472,556 rows, 2013-2023), `local[2]`, 2 vCPU, driver 5 GB.
Reference result, checked against an independent Python implementation (identical 3485 dates):
((38.8, -122.8), (38.8, -122.7)), 3485 co-occurrences, no ties.

| approach   | total s | day-groups s | pair-count s |
|------------|--------:|-------------:|-------------:|
| groupbykey | 447.6   | 6.6          | 436.2        |
| aggregate  | 390.8   | 5.0          | 381.0        |
| pruning    | 12.1    | 4.9          | 2.1 (lower bound) + 0.05 |

Trimmed dataset statistics: 918,872 distinct (cell, day); 3,669 days; 111,361 cells;
up to 474 cells per day (median 249); 117.3 M pairs generated, 64.0 M distinct pairs.
Pruning: only 2 cells are active on >= 3485 days, so the top-64 lower bound is already exact.
