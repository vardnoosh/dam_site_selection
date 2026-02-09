from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pipeline import (
    step01_ingest,
    step02_quality,
    step03_load_postgis,
    step04_terrain,
    step05_constraints,
    step06_candidates,
    step07_features,
    step08_scoring,
    step09_scenarios,
    step10_ranking
)

default_args = {
    "owner": "gis-team",
    "retries": 2,
}

with DAG(
    dag_id="dam_site_selection_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    t1 = PythonOperator(task_id="ingest", python_callable=step01_ingest.run)
    t2 = PythonOperator(task_id="quality", python_callable=step02_quality.run)
    t3 = PythonOperator(task_id="load_postgis", python_callable=step03_load_postgis.run)
    t4 = PythonOperator(task_id="terrain", python_callable=step04_terrain.run)
    t5 = PythonOperator(task_id="constraints", python_callable=step05_constraints.run)
    t6 = PythonOperator(task_id="candidates", python_callable=step06_candidates.run)
    t7 = PythonOperator(task_id="features", python_callable=step07_features.run)
    t8 = PythonOperator(task_id="scoring", python_callable=step08_scoring.run)
    t9 = PythonOperator(task_id="scenarios", python_callable=step09_scenarios.run)
    t10 = PythonOperator(task_id="ranking", python_callable=step10_ranking.run)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10
