#!/usr/bin/env python
import argparse, sys, os, yaml
from pathlib import Path
from utils import *

def main():
    ap = argparse.ArgumentParser(description="Upload local .zst dumps to bronze bucket")
    ap.add_argument("--local-dir", required=True, help="Path to local dumps")
    ap.add_argument("--bucket", required=True, help="Bronze bucket name")
    args = ap.parse_args()
    bucket_name = args.bucket

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
        # layout = load_layout('bronze')  # "bronze/year={YYYY}/month={MM}/{type}-{YYYY}-{MM}.zst"
        # key = layout.format(YYYY=y, MM=m, type=t)

        cfg = load_yaml('./config/storage.yaml')
        layout = cfg['layout']['bronze']
        key = layout.format(YYYY=y, MM=m, type=t)
        print(key)
        
        # if s3.head_object(Bucket=args.bucket, Key=key):
        #     print(f"SKIP: {f} (already exists in bucket)")
        #     continue
        if object_exists(s3, bucket=bucket_name, key=key):
            print(f"SKIP: {f} (already exists in bucket)")
            continue
        print(f"Uploading {f} -> s3://{bucket_name}/{key}")
        s3.upload_file(str(f), bucket_name, key, ExtraArgs={
            "Metadata": {
                "layer":"bronze",
                "type":t or "", 
                "year":y or "", 
                "month":m or ""},
            "ContentType":"application/zstd"
        })
    print("Done.")

if __name__ == "__main__":
    main()
