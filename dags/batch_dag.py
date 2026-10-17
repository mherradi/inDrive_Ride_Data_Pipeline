"""Batch DAG: 
Purpose:
    Load the 3 CSV files into the raw schema.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

import os
from datetime import datetime, timezone


from db import get_connection , run_sql_file


def load_csv_to_raw(table) :
    """Load the 3 CSV files into the raw schema."""

    connection = get_connection() 

    with connection , connection.cursor() as cursor , open( f"/opt/airflow/data/{table}.csv", "r", encoding="utf-8") as f:

        # Read the CSV header to get the column names.
        columns = f.readline().strip()
        # Reset the file pointer to the beginning of the file after reading the header.
        f.seek(0)

        cursor.execute(f"TRUNCATE raw.{table};")

        # FROM STDIN means PostgreSQL receives the CSV data through the client connection.
        cursor.copy_expert(f"COPY raw.{table} ({columns}) FROM STDIN WITH CSV HEADER", f)
        
    cursor.close()


    print(f"Loaded 3 CSV files into the raw schema.")


with DAG(
    dag_id="2_batch",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    for table in ["vehicles", "drivers", "zones"]:
        PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_csv_to_raw,
            op_args=[table],
        )