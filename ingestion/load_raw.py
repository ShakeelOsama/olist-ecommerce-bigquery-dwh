"""
Olist raw ingestion: Kaggle -> GCS landing zone -> BigQuery `raw` dataset.

Design
------
* Raw is schema-on-read: every source column lands as STRING. Typing and
  cleaning happen in the dbt `cleaned` layer, so a bad value in a source file
  never fails the load (it surfaces as a NULL + a failing dbt test instead).
* Every row gets lineage columns: _ingested_at, _source_file, _batch_id.
* Loads are idempotent per batch: re-running the same --batch-id deletes that
  batch's rows first, so a retried Airflow task never duplicates data.
* Raw tables are partitioned by ingestion date so history is kept cheaply and
  the cleaned layer can dedupe to the latest version of each key.

Usage
-----
    python ingestion/load_raw.py --project my-gcp-project --bucket my-landing-bucket
    python ingestion/load_raw.py --project p --bucket b --local-dir ./data   # skip Kaggle download
"""
from __future__ import annotations

import argparse
import csv
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

log = logging.getLogger("olist_ingest")

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

# source file -> raw table name
FILES: dict[str, str] = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}

LINEAGE_COLUMNS = [
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_batch_id", "STRING", mode="REQUIRED"),
]


def download_from_kaggle(target: Path) -> None:
    """Needs ~/.kaggle/kaggle.json (or KAGGLE_USERNAME / KAGGLE_KEY env vars)."""
    target.mkdir(parents=True, exist_ok=True)
    log.info("Downloading %s from Kaggle into %s", KAGGLE_DATASET, target)
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", KAGGLE_DATASET, "-p", str(target), "--unzip"],
        check=True,
    )


def read_header(path: Path) -> list[str]:
    # utf-8-sig strips the BOM that the translation file ships with
    with path.open(encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh))
    return [col.strip().lower() for col in header]


def upload_to_gcs(client: storage.Client, bucket: str, local: Path, batch_id: str) -> str:
    blob_name = f"olist/landing/batch_id={batch_id}/{local.name}"
    client.bucket(bucket).blob(blob_name).upload_from_filename(str(local))
    uri = f"gs://{bucket}/{blob_name}"
    log.info("Uploaded %s", uri)
    return uri


def ensure_raw_table(bq: bigquery.Client, table_id: str, columns: list[str]) -> None:
    schema = [bigquery.SchemaField(c, "STRING") for c in columns] + LINEAGE_COLUMNS
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="_ingested_at", type_=bigquery.TimePartitioningType.DAY)
    table.labels = {"layer": "raw", "source": "olist"}
    bq.create_table(table, exists_ok=True)


def load_file(bq: bigquery.Client, project: str, dataset: str, table: str,
              gcs_uri: str, columns: list[str], batch_id: str) -> int:
    target = f"{project}.{dataset}.{table}"
    staging = f"{project}.{dataset}._stg_{table}_{batch_id.replace('-', '_')}"
    ensure_raw_table(bq, target, columns)

    # 1) load the file as-is (all STRING) into a short-lived staging table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=[bigquery.SchemaField(c, "STRING") for c in columns],
        allow_quoted_newlines=True,          # review comments contain line breaks
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records=0,
    )
    bq.load_table_from_uri(gcs_uri, staging, job_config=job_config).result()
    bq.query(
        f"ALTER TABLE `{staging}` SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))"
    ).result()

    # 2) idempotent append into raw with lineage columns
    col_list = ", ".join(f"`{c}`" for c in columns)
    sql = f"""
    BEGIN TRANSACTION;
      DELETE FROM `{target}` WHERE _batch_id = @batch_id;
      INSERT INTO `{target}` ({col_list}, _ingested_at, _source_file, _batch_id)
      SELECT {col_list}, CURRENT_TIMESTAMP(), @source_file, @batch_id FROM `{staging}`;
    COMMIT TRANSACTION;
    """
    params = [
        bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
        bigquery.ScalarQueryParameter("source_file", "STRING", gcs_uri),
    ]
    bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    bq.delete_table(staging, not_found_ok=True)

    rows = next(iter(bq.query(
        f"SELECT COUNT(*) AS n FROM `{target}` WHERE _batch_id = @batch_id",
        job_config=bigquery.QueryJobConfig(query_parameters=params[:1]),
    ).result())).n
    log.info("Loaded %s rows into %s (batch %s)", f"{rows:,}", target, batch_id)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True, help="GCS landing bucket")
    parser.add_argument("--dataset", default="raw")
    parser.add_argument("--location", default="US")
    parser.add_argument("--local-dir", type=Path, default=Path("data/olist"))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--batch-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.skip_download:
        download_from_kaggle(args.local_dir)

    bq = bigquery.Client(project=args.project, location=args.location)
    gcs = storage.Client(project=args.project)
    bq.create_dataset(bigquery.Dataset(f"{args.project}.{args.dataset}"), exists_ok=True)

    summary: dict[str, int] = {}
    for filename, table in FILES.items():
        path = args.local_dir / filename
        if not path.exists():
            log.error("Missing source file %s", path)
            return 1
        uri = upload_to_gcs(gcs, args.bucket, path, args.batch_id)
        summary[table] = load_file(bq, args.project, args.dataset, table, uri, read_header(path), args.batch_id)

    log.info("Batch %s complete: %s", args.batch_id, summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
