import argparse
import gzip
import json
import os
from collections import Counter
from pathlib import Path
import requests

RAW_DIR = Path("data/raw")
DOCS_DIR = Path("docs")

def download_hour(date_str: str, hour: int) -> Path:
    filename = f"{date_str}-{hour}.json.gz"
    local_path = RAW_DIR / filename
    if local_path.exists():
        print(f"  [skip] {filename} already downloaded")
        return local_path

    url = f"https://data.gharchive.org/{filename}"
    print(f"  [download] {url}")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()  # fail loudly if the file doesn't exist (e.g. future date)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return local_path

def iter_events(gz_path: Path):
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def main(date_str: str):
    type_counts = Counter()
    sample_push_event = None
    sample_pr_event = None
    print(f"Downloading + inspecting {date_str} (24 hourly files)...")
    for hour in range(24):
        gz_path = download_hour(date_str, hour)
        for event in iter_events(gz_path):
            event_type = event.get("type", "UNKNOWN")
            type_counts[event_type] += 1
            if event_type == "PushEvent" and sample_push_event is None:
                sample_push_event = event
            if event_type == "PullRequestEvent" and sample_pr_event is None:
                sample_pr_event = event
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # event type distribution 
    dist_path = DOCS_DIR / "event_type_distribution.json"
    with open(dist_path, "w") as f:
        json.dump(dict(type_counts.most_common()), f, indent=2)
    print(f"\nWrote {dist_path}")
    print("Top event types:")
    for etype, count in type_counts.most_common(5):
        print(f"  {etype}: {count}")

    # structural comparison
    comparison_path = DOCS_DIR / "event_structure_comparison.md"
    with open(comparison_path, "w") as f:
        f.write(f"# Event Structure Comparison — {date_str}\n\n")
        f.write("## PushEvent (full sample)\n\n```json\n")
        f.write(json.dumps(sample_push_event, indent=2) if sample_push_event else "NOT FOUND")
        f.write("\n```\n\n")
        f.write("## PullRequestEvent (full sample)\n\n```json\n")
        f.write(json.dumps(sample_pr_event, indent=2) if sample_pr_event else "NOT FOUND")
        f.write("\n```\n\n")
        f.write("## Notes on structural differences\n\n")
        f.write("- TODO: fill in after reading both payloads -\n")
        f.write("  which fields are shared (actor, repo, created_at)?\n")
        f.write("  which fields exist ONLY in one event type (payload.commits vs payload.pull_request)?\n")
    print(f"Wrote {comparison_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format, e.g. 2015-01-01")
    args = parser.parse_args()
    main(args.date)