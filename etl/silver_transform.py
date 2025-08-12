#!/usr/bin/env python
import argparse, os, sys, json, io
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

def normalize_record(rec:dict):
    out = {}
    out["id"] = rec.get("id")
    out["author"] = rec.get("author")
    out["subreddit"] = rec.get("subreddit")
    ts = rec.get("created_utc")
    try:
        ts = int(ts)
    except Exception:
        ts = None
    out["created_utc"] = ts
    out["body"] = rec.get("body") or rec.get("selftext")
    out["title"] = rec.get("title")
    out["score"] = rec.get("score")
    out["parent_id"] = rec.get("parent_id")
    out["link_id"] = rec.get("link_id")
    out["permalink"] = rec.get("permalink")
    out["url"] = rec.get("url")
    return out

def write_dataset(df, out_dir):
    # Ensure created_utc is int64
    if "created_utc" in df.columns:
        df["created_utc"] = pd.to_numeric(df["created_utc"], errors="coerce").astype("Int64")
        ts = df["created_utc"].dropna().astype("int64")
        df["_year"] = pd.to_datetime(ts, unit="s", utc=True).dt.year
        df["_month"] = pd.to_datetime(ts, unit="s", utc=True).dt.month
    else:
        df["_year"] = None
        df["_month"] = None

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(out_dir),
        partition_cols=["_year","_month"],
        use_dictionary=True,
        compression="zstd"
    )

def process_file(ndjson_path:Path, out_dir:Path, batch_size:int=100_000):
    rows = []
    with open(ndjson_path, "r", encoding="utf-8") as fh:
        for _, line in enumerate(fh, 1):
            try:
                rec = json.loads(line)
            except Exception:
                continue
            rows.append(normalize_record(rec))
            if len(rows) >= batch_size:
                df = pd.DataFrame(rows)
                df.drop_duplicates(subset=["id"], inplace=True, keep="last")
                write_dataset(df, out_dir)
                rows.clear()
        if rows:
            df = pd.DataFrame(rows)
            df.drop_duplicates(subset=["id"], inplace=True, keep="last")
            write_dataset(df, out_dir)

def main():
    ap = argparse.ArgumentParser(description="Transform NDJSON sample/raw into partitioned Parquet (silver)")
    ap.add_argument("--input", required=True, help="Folder with .ndjson files")
    ap.add_argument("--output", required=True, help="Output folder for Parquet dataset")
    args = ap.parse_args()

    in_root = Path(args.input)
    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)

    files = sorted(in_root.glob("*.ndjson"))
    if not files:
        print(f"No .ndjson in {in_root}", file=sys.stderr)
        sys.exit(1)

    for f in files:
        print(f"Transforming {f} -> {out_root}")
        process_file(f, out_root, batch_size=100_000)
    print("Done.")

if __name__ == "__main__":
    main()
