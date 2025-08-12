#!/usr/bin/env python
import argparse, os, sys, json, hashlib
from pathlib import Path
import zstandard as zstd

# Deterministic sample from local raw .zst JSONL files.
# Inclusion rule:
#   md5(str(record[hash_field])) % 10_000 < fraction * 10_000

def should_keep(value:str, frac:float):
    h = hashlib.md5(str(value).encode("utf-8")).hexdigest()
    bucket = int(h[:8], 16) % 10_000
    return bucket < int(frac * 10_000)

def stream_jsonl_zst(path:Path):
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            buf = b""
            while True:
                chunk = reader.read(1<<20)  # 1 MB
                if not chunk:
                    break
                buf += chunk
                while True:
                    nl = buf.find(b"\n")
                    if nl == -1:
                        break
                    line = buf[:nl]
                    buf = buf[nl+1:]
                    if not line.strip():
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue  # skip bad line

def main():
    ap = argparse.ArgumentParser(description="Create deterministic local NDJSON samples from .zst raw files")
    ap.add_argument("--input", required=True, help="Local folder containing .zst dumps")
    ap.add_argument("--output", required=True, help="Output folder for .ndjson samples")
    ap.add_argument("--fraction", type=float, default=0.01, help="Sample fraction (0-1)")
    ap.add_argument("--hash-field", default="id", help="Field name to hash for sampling")
    args = ap.parse_args()

    in_root = Path(args.input)
    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    files = sorted(in_root.glob("**/*.zst"))
    if not files:
        print(f"No .zst under {in_root}", file=sys.stderr)
        sys.exit(1)

    for f in files:
        basename = f.name.replace(".zst", "")  # avoid double .ndjson.zst
        out_path = out_root / f"{basename}.ndjson"
        kept = 0
        total = 0
        with open(out_path, "w", encoding="utf-8") as out_f:
            for rec in stream_jsonl_zst(f):
                total += 1
                key = rec.get(args.hash_field, None)
                if key is None:
                    continue
                if should_keep(key, args.fraction):
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    kept += 1
        print(f"Wrote {out_path}  kept={kept:,}  from={total:,}")
    print("Done.")

if __name__ == "__main__":
    main()
