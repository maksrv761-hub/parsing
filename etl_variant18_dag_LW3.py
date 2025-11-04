# -*- coding: utf-8 -*-
"""
Airflow DAG — ЛР3, Вариант 18
Кейс: выявить товары, которых не хватает на складах для выполнения заказов.

Источник данных (должны лежать в /opt/airflow/dags/data/):
- warehouses.csv: warehouse_id, city               (не обязателен для расчёта дефицита)
- products.xlsx: product_id, warehouse_id, quantity
- orders.json:   order_id, product_id, quantity_to_ship

Результат:
- shortages.csv в /opt/airflow/dags/data/
- загрузка таблицы shortages в SQLite /opt/airflow/dags/data/warehouse.db
- EmailOperator отправляет письмо со вложением shortages.csv (через smtp_default, MailHog)

DAG совместим с Airflow 2.x.
"""

from __future__ import annotations

import os
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email_smtp  # на случай прямого вызова
from airflow.models.baseoperator import chain

# === Пути внутри контейнера Airflow ===
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
WAREHOUSES_CSV = os.path.join(DATA_DIR, "warehouses.csv")
PRODUCTS_XLSX  = os.path.join(DATA_DIR, "products.xlsx")
ORDERS_JSON    = os.path.join(DATA_DIR, "orders.json")
OUTPUT_CSV     = os.path.join(DATA_DIR, "shortages.csv")
SQLITE_DB      = os.path.join(DATA_DIR, "warehouse.db")

# === Параметры DAG ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="etl_variant18_shortages",
    description="Variant 18: Identify stock shortages per product vs. orders; load to SQLite; email report",
    default_args=default_args,
    schedule_interval=None,     # без расписания; запуск вручную
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab3", "variant18", "etl"],
)

# === Хелперы ===
def _ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

def _assert_exists(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Не найден входной файл: {path}")

# === Extract ===
def extract_products(**context: Dict[str, Any]) -> str:
    """Читает products.xlsx, нормализует колонки и сохраняет во временный CSV; возвращает путь."""
    _ensure_data_dir()
    _assert_exists(PRODUCTS_XLSX)
    df = pd.read_excel(PRODUCTS_XLSX)
    # нормализация
    df = df.rename(columns={c: c.strip().lower() for c in df.columns})
    required = {"product_id", "warehouse_id", "quantity"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"В products.xlsx отсутствуют колонки: {missing}")

    tmp_path = os.path.join(DATA_DIR, "tmp_products.csv")
    df.to_csv(tmp_path, index=False)
    context["ti"].xcom_push(key="products_path", value=tmp_path)
    return tmp_path

def extract_orders(**context: Dict[str, Any]) -> str:
    """Читает orders.json и сохраняет нормализованный CSV; возвращает путь."""
    _ensure_data_dir()
    _assert_exists(ORDERS_JSON)
    with open(ORDERS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df = df.rename(columns={c: c.strip().lower() for c in df.columns})
    required = {"order_id", "product_id", "quantity_to_ship"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"В orders.json отсутствуют поля: {missing}")

    tmp_path = os.path.join(DATA_DIR, "tmp_orders.csv")
    df.to_csv(tmp_path, index=False)
    context["ti"].xcom_push(key="orders_path", value=tmp_path)
    return tmp_path

# === Transform ===
def transform_compute_shortages(**context: Dict[str, Any]) -> str:
    """Объединяет остатки и заказы → считает дефицит → сохраняет shortages.csv; возвращает путь."""
    _ensure_data_dir()
    ti = context["ti"]
    products_path = ti.xcom_pull(key="products_path", task_ids="extract_products")
    orders_path   = ti.xcom_pull(key="orders_path", task_ids="extract_orders")

    if not products_path or not os.path.exists(products_path):
        raise FileNotFoundError("Не найден tmp-файл с products")
    if not orders_path or not os.path.exists(orders_path):
        raise FileNotFoundError("Не найден tmp-файл с orders")

    products = pd.read_csv(products_path)
    orders   = pd.read_csv(orders_path)

    # суммарный складской остаток по продукту
    stock = products.groupby("product_id", as_index=False)["quantity"].sum() \
                    .rename(columns={"quantity": "stock_total"})
    # суммарный спрос (к отгрузке) по продукту
    demand = orders.groupby("product_id", as_index=False)["quantity_to_ship"].sum() \
                   .rename(columns={"quantity_to_ship": "orders_total"})

    # объединение
    result = pd.merge(demand, stock, on="product_id", how="left")
    result["stock_total"] = result["stock_total"].fillna(0).astype(int)
    result["orders_total"] = result["orders_total"].astype(int)

    # дефицит = max(0, orders_total - stock_total)
    result["deficit"] = (result["orders_total"] - result["stock_total"]).clip(lower=0).astype(int)
    result["computed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # сортировка для читаемости
    result = result.sort_values(["deficit", "product_id"], ascending=[False, True])

    # запись финального отчёта
    result.to_csv(OUTPUT_CSV, index=False)
    ti.xcom_push(key="shortages_csv", value=OUTPUT_CSV)
    return OUTPUT_CSV

# === Load ===
def load_to_sqlite(**context: Dict[str, Any]) -> str:
    """Загружает shortages.csv в SQLite таблицу 'shortages'."""
    _ensure_data_dir()
    ti = context["ti"]
    csv_path = ti.xcom_pull(key="shortages_csv", task_ids="transform_compute_shortages")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError("Не найден финальный CSV с дефицитами")

    df = pd.read_csv(csv_path)
    with sqlite3.connect(SQLITE_DB) as conn:
        df.to_sql("shortages", conn, if_exists="replace", index=False)

    # проверочный select для логов
    with sqlite3.connect(SQLITE_DB) as conn:
        check = pd.read_sql_query("SELECT COUNT(*) AS cnt FROM shortages", conn)
        print("Rows loaded to SQLite shortages:", int(check["cnt"][0]))

    return SQLITE_DB

# === Email (через smtp_default; MailHog ловит письма на 1025) ===
email_task = EmailOperator(
    task_id="notify",
    to="test@example.com",
    subject="[Airflow] Variant 18 ETL complete",
    html_content="""
    <h3>ETL pipeline finished successfully</h3>
    <p>Variant 18 (shortages report) has been generated and loaded to SQLite.</p>
    """,
    files=[OUTPUT_CSV],
    dag=dag,
)

# === Операторы ===
t_extract_products = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products,
    provide_context=True,
    dag=dag,
)

t_extract_orders = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders,
    provide_context=True,
    dag=dag,
)

t_transform = PythonOperator(
    task_id="transform_compute_shortages",
    python_callable=transform_compute_shortages,
    provide_context=True,
    dag=dag,
)

t_load = PythonOperator(
    task_id="load_to_sqlite",
    python_callable=load_to_sqlite,
    provide_context=True,
    dag=dag,
)

# === Граф зависимостей ===
chain([t_extract_products, t_extract_orders], t_transform, t_load, email_task)

# Подсказка: проверьте, что SMTP настроен (smtp_default) и MailHog слушает порт 1025/8025 в docker-compose.
# В Docker-сборке из методички MailHog уже входит в состав.
