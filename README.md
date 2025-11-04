
# Практическая работа №4 — Вариант 18: Аналитический дашборд по качеству вина

**Студент:** Рыбаков Максим (БД-241м)  
**GitHub:** https://github.com/maksrv18/pr4_variant18_wine

## 1. Цель работы
Построить ETL‑пайплайн в Apache Airflow для загрузки и агрегирования данных о качестве вина (красное/белое) в PostgreSQL,
и разработать дашборд в Apache Superset для визуальной аналитики основных показателей.

## 2. Исследуемые метрики бизнес‑процесса
- Распределение *quality* по типу вина (red/white).  
- Средний уровень *alcohol* по различным значениям *quality*.  
- Средние показатели *sulphates*, *residual_sugar* по *quality*.  
- Связь *alcohol* и *quality* (scatter).

## 3. Архитектура решения
1) **Airflow DAG**: `create_tables → load_csv_to_postgres → build_agg → export_agg_to_csv`.  
2) **PostgreSQL** (schema: `wine`): сырьё `wine_quality_raw`, агрегаты `wine_quality_agg`.  
3) **Superset**: подключение к Postgres, построение графиков и сборка дашборда.

![DAG](assets/dag_graph.png)

## 4. Технологический стек
Apache Airflow 2.9, PostgreSQL 15, Apache Superset 4.1, Python 3.11, Pandas, Docker Compose.

## 5. Git‑репозиторий содержит
- DAG: `dags/pr4_variant18_wine_quality.py`  
- SQL: `sql/01_ddl.sql`, `sql/02_build_agg.sql`  
- Экспорт дашборда: `superset/wine_dashboard.json`  
- Данные: `data/wine_quality.csv`  
- Результаты/графики: `results/*.png`, `results/correlation_matrix.csv`

## 6. Описание проделанной работы
- Реализован и проверен DAG (см. граф выше).  
- Загружены данные из CSV → PostgreSQL (raw), построена агрегированная витрина.  
- Подготовлены визуализации и дашборд в Superset.

![Dashboard](assets/dashboard_preview.png)

## 7. Выводы по анализу
- *Quality* положительно связано с *alcohol* и *citric_acid*, отрицательно — с *volatile_acidity*.  
- Для белых вин распределение качества выше, чем для красных (на тестовом наборе).  
- Витрина `wine_quality_agg` ускоряет построение отчётности и упрощает SQL‑запросы.

### Запуск
```bash
docker compose up -d
# Airflow: http://localhost:8080  (admin/admin)
# Superset: http://localhost:8088
```
В Airflow запустить DAG: **pr4_variant18_wine_quality**.  
В Superset подключить БД: `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow` и импортировать таблицы `wine.wine_quality_raw`, `wine.wine_quality_agg`.
