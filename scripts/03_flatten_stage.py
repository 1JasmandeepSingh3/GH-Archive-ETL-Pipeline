import argparse
from pathlib import Path
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
DB_PATH = Path("data/gh_archive.duckdb")
MAX_STRING_LEN = 255

def build_spark():
    return (
        SparkSession.builder
        .appName("gh-archive-flatten")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def flatten(df):
    flattened = df.select(
        F.col("id").cast("string").alias("event_id"),
        F.col("type").alias("event_type"),
        F.substring(F.col("actor.login"), 1, MAX_STRING_LEN).alias("actor_login"),
        F.col("actor.id").cast("long").alias("actor_id"),
        F.substring(F.col("repo.name"), 1, MAX_STRING_LEN).alias("repo_name"),
        F.col("repo.id").cast("long").alias("repo_id"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
        F.when(F.col("type") == "PushEvent", F.size(F.col("payload.commits")))
         .otherwise(None).alias("push_commit_count"),
        F.when(F.col("type") == "PullRequestEvent", F.col("payload.action"))
         .otherwise(None).alias("pr_action"),
        F.when(F.col("type") == "PullRequestEvent", F.col("payload.pull_request.number"))
         .otherwise(None).alias("pr_number"),

        F.to_json(F.col("payload")).alias("payload_json"),
    )
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

    import gc
    import time
    gc.collect()
    time.sleep(3)
    
    print("Loading into DuckDB...")
    con = duckdb.connect(str(DB_PATH))
    con.execute("SET memory_limit='2GB'")
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