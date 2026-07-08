# GH Archive ELT Pipeline

An incremental ELT pipeline that ingests raw GitHub event data from "GH Archive" (https://www.gharchive.org/), models it into a star schema, validates it with data-quality checks, and serves it through a set of analytical tables.

## Architecture

raw data (gzipped JSON data)  -->  staged data (typed & flattened)  -->  star schema (dimensions & fact table) -->  serving layer (with analytical queries)


Three distinct storage layers, no skipping straight from raw to a report:

1. **Raw** — hourly JSON files downloaded directly from GH Archive.
2. **Staging** — nested JSON flattened into a typed layer via PySpar, with schema drift and missing fields handled explicitly.
3. **Model** — a star schema (`fact_events` + `dim_actor`, `dim_repo`,
   `dim_date`) with a Type-2 SCD on `dim_repo`.
4. **Serving** — 4 queries answering specific analytical questions, refreshed via atomic swap, no UPDATE.

## Scale

- 7 consecutive days of GH Archive data; 2025-01-01 to 2025-01-07. (168 hourly files)
- ~32M raw events loaded into staging.
- ~30.7M events in the final fact table after exclusion of 9 quarentined partitions.

## Repo structure

schema/
  dim_actor.sql          -- actor dimension
  dim_date.sql           -- date dimension
  dim_repo_scd2.sql      -- repo dimension (Type-2 SCD)
  fact_events.sql        -- fact table about the events

scripts/
  01_inspect_raw.py      -- inspect raw hourly files
  02_inspect_raw.py      -- further raw data inspection
  03_flatten_stage.py    -- Flattening of nested JSON into staged_events through Pyspark.
  build_model.py         -- builds dim/fact tables through SQL queries, gates fact_events on quarantine.
  data_quality_check.py  -- reconciliation, uniqueness, SCD-2 overlap, freshness, quarantine check.
  build_serving.py       -- builds the serving layer, with 4 Analytical queries, via atomic swap.
  view_serving_tables.py -- CLI viewer for the serving table Analys=tical Queries.
  clear_log_for_date.py  -- clears load_log watermark entries for a date range.
  actor_duplicates.py    -- diagnose duplicate rows in  dim_actor per actor_id
  repo_overlap.py        -- diagnose overlapping valid_from/valid_to windows in dim_repo (SCD-2 fan-out bug)

docs/
  event_structure_comparison.md  -- PushEvent vs PullRequestEvent payload structure notes
  event_type_distribution.json   -- event type counts from initial inspection

data/                    -- gitignored, holds the local DuckDB file and raw/staged data

## How to run

### 1. Ingest + flatten
python scripts/01_inspect_raw.py
python scripts/02_inspect_raw.py
python scripts/03_flatten_stage.py

### 2. Build the star schema/Quarentine detection 
python scripts/build_model.py

### 3. Run the data-quality stage independently
python scripts/data_quality_check.py

### 4. Build the serving layer/Analytical queries
python scripts/build_serving.py

### 5. Watch output of Serving layer Queries
python scripts/view_serving_tables.py


Re-running any ingestion step for an already-loaded date range is a no-op — the watermark in `load_log` prevents double-counting. To force a re-load for a specific range, clear its watermark first:

python scripts/clear_log_for_date.py --start-date 2025-01-01 --end-date 2025-01-01


## dim_repo as a Type-2 SCD

Repositories get renamed and change owners over time. `dim_repo` preserves this history with `valid_from` / `valid_to` / `is_current` columns rather than overwriting a repo's name in place. `fact_events` joins to `dim_repo` on the key `repo_id` plus a time-bounded range check 
`event_timestamp BETWEEN valid_from AND valid_to`, so a historical event always resolves to whichever version of the repo was current when the event actually happened, not the repo's current name.

## Data quality stage

- **Reconciliation** — staging row count vs. fact row count, adjusted to exclude quarantined hours 
  (a quarantined hour is supposed to be missing from the fact table, so the check compares against the non-quarantined subset of staging rather than raw staging).
- **Uniqueness** — no duplicate surrogate keys in any dimension tables.
- **SCD-2 overlap check** — no two `dim_repo` rows for the same `repo_id` have overlapping 
  `valid_from`/  `valid_to` windows.
- **Freshness** — checks how far behind "now" the most recent loaded partition is.
   Since the project loads a fixed historical 7-day window rather than a continuously-updating feed, this check will always report `FAIL` against this dataset by design.

## Trade-Offs
Any duplicate `event_id` currently quarantines the entire hour, even though most duplicates are harmless redelivery rather than real corruption. A stricter version would only quarantine hours with `NULL` key columns (real corruption) and simply deduplicate hours whose only issue is a repeated `event_id`; i.e keeping one copy per event; rather than discarding the whole hour. This was scoped out for time, and is noted here as a deliberate simplification rather than an oversight.

## Quarantine

Before `fact_events` is built, every hourly partition is checked for duplicate `event_id`s.
Of 168 total hourly partitions, **9 were quarantined** and **159 passed**. Quarantined hours are logged to `quarantine_log` and excluded from `fact_events` via a `NOT EXISTS` check. A failing partition is set aside, not silently merged in, and the other 159 flow through untouched.

## Serving layer

Four analytical questions, each refreshed via **atomic swap** — built fully under a temporary table name, validated non-empty, then swapped into place with an atomic rename. A crash mid-refresh leaves the live
table completely untouched, rather than partially updated.

| Table | Question |
|---|---|
| `weekly_active_repos` | Distinct active repos per week |
| `pr_merge_latency` | Hours between a PR's `opened` and `closed` action |
| `top_event_types` | Event volume by type |
| `stars_per_day` | Daily count of `WatchEvent` (GH Archive's stand-in for a star) |