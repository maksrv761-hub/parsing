
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
RAW_CSV = os.path.join(DATA_DIR, "wine_quality.csv")

default_args = {"owner":"airflow","depends_on_past":False,"email_on_failure":False,"email_on_retry":False,"retries":1,"retry_delay":timedelta(minutes=2)}

dag = DAG(
    dag_id="pr4_variant18_wine_quality",
    description="Load wine quality CSV -> Postgres -> aggregate -> export for Superset",
    schedule=None,
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["pr4","variant18","wine","etl","superset"]
)

def load_csv_to_postgres(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(RAW_CSV)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS wine;")
        df.to_sql("wine_quality_raw", conn, schema="wine", if_exists="replace", index=False)

def export_agg_to_csv(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df("SELECT * FROM wine.wine_quality_agg ORDER BY type, quality")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    df.to_csv(os.path.join(RESULTS_DIR, "wine_quality_agg.csv"), index=False)

create_tables = PostgresOperator(task_id="create_tables", postgres_conn_id="postgres_default", sql="sql/01_ddl.sql", dag=dag)
load_csv = PythonOperator(task_id="load_csv_to_postgres", python_callable=load_csv_to_postgres, provide_context=True, dag=dag)
build_agg = PostgresOperator(task_id="build_agg", postgres_conn_id="postgres_default", sql="sql/02_build_agg.sql", dag=dag)
export_csv = PythonOperator(task_id="export_agg_to_csv", python_callable=export_agg_to_csv, provide_context=True, dag=dag)

chain(create_tables, load_csv, build_agg, export_csv)
