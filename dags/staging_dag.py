"""Staging DAG :

Purpose:
    Wait for streaming + batch, then raw -> staging (clean, cast types). Then starts the marts DAG. 

"""

from db import run_sql_file , get_connection
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Represents an execution of a DAG
from airflow.models import DagRun

# To Create dependencies between two groups of tasks
from airflow.models.baseoperator import cross_downstream

# Triggers another DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Repeatedly checks a condition
from airflow.sensors.python import PythonSensor


def latest_run_succeeded(dag_id):
    """Wait for the newest upstream run to finish successfully."""
    runs = DagRun.find(dag_id=dag_id)
    if not runs:
        return False
    latest = max(runs, key=lambda run: run.execution_date)
    return latest.state == "success"


with DAG(
    dag_id   = "3_staging",
    schedule =  "*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inDrive"],

) as dag:
    # GIVEN: 2 sensors. They wait until "1_streaming" and "2_batch" have each succeeded once.

    waits = [
        PythonSensor(
            task_id=f"wait_{name}",
            python_callable=latest_run_succeeded,
            op_args=[dag_id],
            mode="reschedule",
            poke_interval=10,
            timeout=90,
            # nothing to stage yet -> skip this run, try again in 2 min
            soft_fail=True
        )

        for name , dag_id in [("streaming", "1_streaming"), ("batch", "2_batch")]
    ]

    staging_tasks = [
        PythonOperator(
            task_id=f"staging_{table}",
            python_callable=run_sql_file,
            op_args=[f"staging/{table}.sql"],
        )
        for table in ["drivers", "rides", "vehicles", "zones"]
    ]

    starts_marts = TriggerDagRunOperator(
        task_id="start_marts",
        trigger_dag_id="4_marts"
    )

    cross_downstream(waits, staging_tasks)
    staging_tasks >> starts_marts    
