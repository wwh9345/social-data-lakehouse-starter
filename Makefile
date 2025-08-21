SHELL := /bin/bash
.ONESHELL:

ifneq (,$(wildcard ./.env))
  include .env
  export
endif

PY := python3

.PHONY: setup
setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip
# 	. .venv/bin/activate && pip install -r requirements.txt
	. .venv/bin/activate && pip install -r requirements_full.txt

.PHONY: upload
upload:
	. .venv/bin/activate && $(PY) etl/ingest_upload.py \
		--local-dir "$(RAW_DIR)" \
		--bucket "$(BUCKET_BRONZE)"

.PHONY: upload_prototype
upload_prototype:
	. .venv/bin/activate && $(PY) etl/PROTO_ingest_upload.py \
		--local-dir "$(RAW_DIR)" \
		--bucket "$(BUCKET_BRONZE)" \
		--period-months "$(PERIOD_MONTHS)"

.PHONY: sample
sample:
	. .venv/bin/activate && $(PY) etl/sample_create.py \
		--input "$(RAW_DIR)" \
		--output "$(OUT_DIR)" \
		--fraction "$(FRACTION)"

.PHONY: silver
silver:
	. .venv/bin/activate && $(PY) etl/silver_transform.py \
		--input "$(IN_DIR)" \
		--output "$(OUT_DIR)"

.PHONY: upload-silver
upload-silver:
	. .venv/bin/activate && $(PY) etl/s3_upload_folder.py \
		--folder "$(FOLDER)" \
		--bucket "$(BUCKET_SILVER)" \
		--prefix "$(PREFIX)"
