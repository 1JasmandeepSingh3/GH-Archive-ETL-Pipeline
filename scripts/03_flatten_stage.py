import argparse
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
DB_PATH = Path("data/gh_archive.duckdb")

# Defensive limit for the "oversized string" edge case the brief calls out.
# Real GitHub usernames/repo names are short, but we truncate defensively
# rather than trust that assumption blindly.
MAX_STRING_LEN = 255


def build_spark():
    return (
        SparkSession.builder
        .appName("gh-archive-flatten")
        .master("local[*]")
        # Java 18+ (including Java 23) restricts the security manager by
        # default, which breaks Hadoop internals that Spark depends on even
        # in local mode. This flag re-allows the old behavior instead of
        # requiring a Java downgrade.
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        # Default driver memory (1GB) was too small and crashed. But on an
        # 8GB machine, 4GB left too little for DuckDB + OS afterward - so
        # this is tuned down to leave headroom for the DuckDB step that
        # runs after Spark finishes.
        .config("spark.driver.memory", "2g")
        # Fewer, larger shuffle partitions for a single day's worth of data
        # on a local machine - the 200-partition default is tuned for real
        # clusters, not a laptop processing one day at a time.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def flatten(df):
    """
    Turn nested raw event rows into a flat, typed DataFrame.
    Every column here is deliberately chosen - see Option A strategy above.
    """
    flattened = df.select(
        # --- Envelope fields: exist on every event type ---
        F.col("id").cast("string").alias("event_id"),
        F.col("type").alias("event_type"),

        # actor.login / actor.id are NESTED - dot notation reaches inside
        # the struct. If a field is truly absent from an event, Spark
        # returns NULL here automatically (it doesn't crash), because the
        # schema was inferred across ALL files, so the column exists even
        # if empty for some rows.
        F.substring(F.col("actor.login"), 1, MAX_STRING_LEN).alias("actor_login"),
        F.col("actor.id").cast("long").alias("actor_id"),

        F.substring(F.col("repo.name"), 1, MAX_STRING_LEN).alias("repo_name"),
        F.col("repo.id").cast("long").alias("repo_id"),

        # created_at arrives as a string like "2025-01-01T00:00:05Z" -
        # cast it to a real timestamp type now, not later.
        F.to_timestamp(F.col("created_at")).alias("created_at"),

        # --- Narrow payload fields for the two most common event types ---
        # size(...) returns NULL (not an error) for event types that don't
        # have a "commits" array at all - e.g. an IssuesEvent.
        F.when(F.col("type") == "PushEvent", F.size(F.col("payload.commits")))
         .otherwise(None).alias("push_commit_count"),

        F.when(F.col("type") == "PullRequestEvent", F.col("payload.action"))
         .otherwise(None).alias("pr_action"),

        F.when(F.col("type") == "PullRequestEvent", F.col("payload.pull_request.number"))
         .otherwise(None).alias("pr_number"),

        # --- Escape hatch: keep everything else as raw JSON text ---
        F.to_json(F.col("payload")).alias("payload_json"),
    )

    # Drop rows that are missing the one field nothing should ever be
    # missing - a valid event_id. This is a MINIMAL sanity filter, not
    # full data-quality validation (that's Week 3's dedicated stage).
    flattened = flattened.filter(F.col("event_id").isNotNull())

    return flattened


def main(date_str: str):
    spark = build_spark()

    raw_glob = str(RAW_DIR / f"{date_str}-*.json.gz")
    print(f"Reading raw files matching: {raw_glob}")
    raw_df = spark.read.json(raw_glob)

    print(f"Raw row count: {raw_df.count()}")

    staged_df = flatten(raw_df)
    print(f"Staged row count: {staged_df.count()}")

    out_path = STAGED_DIR / date_str
    print(f"Writing Parquet to: {out_path}")
    staged_df.write.mode("overwrite").parquet(str(out_path))

    spark.stop()

    # Give the OS a moment to actually reclaim the JVM's memory before
    # DuckDB starts asking for its own - on an 8GB machine this gap matters.
    import gc
    import time
    gc.collect()
    time.sleep(3)

    # --- Load the Parquet output into DuckDB as a real table ---
    print("Loading into DuckDB...")
    con = duckdb.connect(str(DB_PATH))
    # Explicitly cap DuckDB's memory instead of trusting its default (which
    # can assume more RAM is available than this machine actually has free
    # at this point in the script).
    con.execute("SET memory_limit='2GB'")
    # VIEW instead of TABLE: DuckDB queries the Parquet files directly on
    # disk instead of copying all rows into the .duckdb file. This avoids
    # duplicating storage (Parquet + a full copy inside DuckDB) - important
    # on a disk that's already tight on space.
    con.execute(f"""
        CREATE OR REPLACE VIEW staged_events AS
        SELECT * FROM read_parquet('{out_path}/*.parquet')
    """)
    row_count = con.execute("SELECT COUNT(*) FROM staged_events").fetchone()[0]
    print(f"DuckDB table 'staged_events' created: {row_count} rows")
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.date)