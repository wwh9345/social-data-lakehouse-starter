#!/usr/bin/env python
import argparse, sys, os, yaml
from pathlib import Path
from utils import *

# ======================================================================================
# Use this for small, temporary changes to the file-uploading, e.g. textual changes
# ======================================================================================
def main():
    ap = argparse.ArgumentParser(description="Upload local .zst dumps to bronze bucket")
    ap.add_argument("--local-dir", required=True, help="Path to local dumps")
    ap.add_argument("--bucket", required=True, help="Bronze bucket name")
    ap.add_argument("--period-months", required=True, help="Indicate specified time period of data (in months)")
    args = ap.parse_args()
    bucket_name = args.bucket
    period_months = args.period_months

    s3 = s3_client(layer="bronze")
    root = Path(args.local_dir)
    files = sorted(root.glob("**/*.zst"))
    if not files:
        print(f"No .zst files under {root}", file=sys.stderr)
        sys.exit(1)

    for f in files:
        y, m = parse_date_from_name(f.name)
        t = infer_type(f.name)
        if not y or not m:
            print(f"SKIP: {f} (no YYYY-MM in name)")
            continue
        
        # 1. manual key editing. Can use this for quick upload prototyping
        # key = f"bronze/year={y}/month={m}/{t}-{y}-{m}.zst"
        
        # 2. following config in storage.yaml
        cfg = load_yaml('./config/storage.yaml')
        layout = cfg['layout']['bronze']
        # key = layout.format(YYYY=y, MM=m, type=t)
        key = layout.format(YYYY=y, MM=period_months, type=t)
        print(key)
        f_name = f.name.split('/')[-1]
        
        if object_exists(s3, bucket=bucket_name, key=key):
            print(f"SKIP: {f_name} (already exists in bucket)")
            continue
        print(f"Uploading {f_name} -> r2://{bucket_name}/{key}")
        s3.upload_file(str(f), bucket_name, key, ExtraArgs={
            "Metadata": {
                "layer":"bronze",
                "type":t or "", 
                "year":y or "", 
                "month":period_months or ""},
            "ContentType":"application/zstd"
        })
    print("Done.")

if __name__ == "__main__":
    main()
