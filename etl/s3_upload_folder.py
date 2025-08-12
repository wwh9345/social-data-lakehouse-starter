#!/usr/bin/env python
import argparse
from pathlib import Path
from utils import *

def main():
    ap = argparse.ArgumentParser(description="Upload a local folder to s3://bucket/prefix")
    ap.add_argument("--folder", required=True, help="Local folder")
    ap.add_argument("--bucket", required=True, help="Bucket name")
    ap.add_argument("--prefix", default="", help="Key prefix (e.g., silver)")
    args = ap.parse_args()

    s3 = s3_client(layer="silver")
    root = Path(args.folder)
    files = [p for p in root.rglob("*") if p.is_file()]
    for f in files:
        rel = f.relative_to(root).as_posix()
        key = f"{args.prefix.rstrip('/')}/{rel}" if args.prefix else rel
        print(f"Uploading {f} -> s3://{args.bucket}/{key}")
        extra = {}
        if f.suffix.lower() == ".parquet":
            extra["ContentType"] = "application/octet-stream"
        s3.upload_file(str(f), args.bucket, key, ExtraArgs=extra)
    print("Done.")

if __name__ == "__main__":
    main()
