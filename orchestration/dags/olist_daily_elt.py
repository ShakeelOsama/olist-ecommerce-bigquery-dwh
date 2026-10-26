"""
Daily Olist ELT on Cloud Composer (Airflow 2.x).

ingest_raw -> source_freshness -> build_cleaned (run + test) -> build_business (run + test) -> docs

Each dbt step uses `dbt build`, so a failing test stops downstream models from
building on bad data (the business layer is never refreshed from a broken cleaned layer).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT = Variable.get("gcp_project", default_var="your-gcp-project")
BUCKET = Variable.get("olist_landing_bucket", default_var=f"{PROJECT}-olist-landing")
REPO = "/home/airflow/gcs/data/olist-ecommerce-dwh"   # repo synced to the Composer bucket
DBT = f"cd {REPO}/dbt && dbt --no-use-colors"
DBT_FLAGS = "--target prod --profiles-dir ."

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-eng@example.com"],
}

with DAG(
    dag_id="olist_daily_elt",
    description="Olist marketplace: GCS -> raw -> cleaned -> business",
    schedule="0 3 * * *",                       # 03:00 UTC, after the nightly source extract
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["olist", "bigquery", "dbt"],
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            f"python {REPO}/ingestion/load_raw.py --project {PROJECT} --bucket {BUCKET} "
            "--local-dir /tmp/olist --batch-id {{ ts_nodash }}"
        ),
    )

    deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps")

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT} source freshness {DBT_FLAGS}",
    )

    build_cleaned = BashOperator(
        task_id="build_cleaned",
        bash_command=f"{DBT} build --select path:models/cleaned {DBT_FLAGS}",
    )

    build_business = BashOperator(
        task_id="build_business",
        bash_command=f"{DBT} build --select path:models/business path:tests {DBT_FLAGS}",
    )

    docs = BashOperator(
        task_id="generate_docs",
        bash_command=f"{DBT} docs generate {DBT_FLAGS}",
        trigger_rule="all_success",
    )

    ingest_raw >> deps >> source_freshness >> build_cleaned >> build_business >> docs
