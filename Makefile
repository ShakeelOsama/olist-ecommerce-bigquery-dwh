# Usage: make setup && make ingest && make build
# Requires: GCP_PROJECT env var, `gcloud auth application-default login`, Kaggle API key.
GCP_PROJECT ?= $(shell gcloud config get-value project 2>/dev/null)
BUCKET      ?= $(GCP_PROJECT)-olist-landing
TARGET      ?= dev

export GCP_PROJECT

.PHONY: setup infra ingest build test docs clean

setup:
	pip install -r requirements.txt
	cp -n dbt/profiles.yml.example dbt/profiles.yml || true
	cd dbt && dbt deps

infra:
	cd infra/terraform && terraform init && terraform apply -var="project_id=$(GCP_PROJECT)"

ingest:
	python ingestion/load_raw.py --project $(GCP_PROJECT) --bucket $(BUCKET)

build:
	cd dbt && dbt build --target $(TARGET) --profiles-dir .

test:
	cd dbt && dbt test --target $(TARGET) --profiles-dir .

docs:
	cd dbt && dbt docs generate --target $(TARGET) --profiles-dir . && dbt docs serve --profiles-dir .

clean:
	cd dbt && dbt clean
