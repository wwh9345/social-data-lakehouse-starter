# social-media-datalake-starter
PROGRESS: Work In Progress / Under Development

A minimal, provider-agnostic starter to manage Reddit dumps with an **object storage data lake** following the Medallion Architecture:
- **bronze**: raw .zst JSONL dumps (immutable)
- **silver**: cleaned, column-pruned, partitioned Parquet
- **gold** (optional): task-specific features/marts

Assumes the starting point is a **local directory** with monthly Reddit `.zst` (NDJSON-compressed) dump files (sourced from Pushshift).

## Objectives to establish
1. Source of truth in object storage (bronze), immutable.
2. Separation of concerns: storage vs compute vs logic.
3. Deterministic sampling (via hash IDs).
4. Columnar by default (Parquet, partition by year/month).
5. Minimal Costs: Structured for efficient SQL engine querying
6. Reproducibility: everything is scripted via small, repeated steps.

## Layout
```
social-media-datalake-starter/
├─ README.md
├─ Makefile
├─ requirements.txt
├─ .env.sample
├─ config/
│  ├─ data_contract.yaml
│  ├─ sampling.yaml
│  └─ storage.yaml
├─ etl/
│  ├─ ingest_upload.py
│  ├─ sample_create.py
│  ├─ silver_transform.py
│  └─ s3_upload_folder.py
└─ notebooks/
```

## Quick start
1) Create venv + install
```bash
make setup
```
2) Copy `.env.sample` to `.env` and fill your S3-compatible endpoint + keys.
3) Upload raw dumps to **bronze**
```bash
make upload RAW_DIR=/path/to/local/reddit_dumps
```
4) Create a deterministic sample locally
```bash
make sample RAW_DIR=/path/to/local/reddit_dumps FRACTION=0.01 OUT_DIR=./data/samples
```
5) Build first **silver** Parquet from the sample
```bash
make silver IN_DIR=./data/samples OUT_DIR=./data/silver
```
6) (Optional) Upload silver
```bash
make upload-silver FOLDER=./data/silver PREFIX=silver
```

## Buckets
Create two buckets in your provider console:
- `reddit-bronze` (enable **versioning**)
- `reddit-silver` (no versioning by default)
