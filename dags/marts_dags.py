""" Marts: 
Purpose :
    staging -> mart (business tables). Started by the staging DAG, no schedule of its own.
"""


from airflow import DAG
from airflow.operators.python import PythonOperator

from db import run_sql_file
from datetime import datetime


def sql_task(task):

    return PythonOperator(
        task_id=f"mart_{task}",
        python_callable=run_sql_file,   
        op_args=[f"marts/{task}.sql"]
    )

with DAG(
    dag_id="4_marts",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inDrive"],
) as dag:
    sql_task("rides") >> [sql_task(name) for name in ["revenue_by_zone", "driver_leaderboard"]]
