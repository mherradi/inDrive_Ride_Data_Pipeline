""" Streaming DAG: 
Purpose :
    Every minute, call an API and store the answer in raw.checkouts.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import requests
from uuid import uuid4

from datetime import datetime, timezone
from db import get_connection , run_sql_file


def pull_rides() :
    """Call the Mockaroo API and store the answer in raw.rides_stream."""

    # 1. Requests  records from the Mockaroo API
    url = "https://my.api.mockaroo.com/users.json?key=334ee450"

    # 2. Dynamically toggle SSL verification based on VERIFY_SSL == "true"
    verify = os.environ.get("VERIFY_SSL", "true").lower() == "true"

    # 3. Make API request and extract the "carts" payload key
    
    response = requests.get(
        url, 
        timeout=15, 
        verify=verify
    )
    response.raise_for_status()

    rides = response.json()

    # 4. Captures the current UTC timestamp as a naive Python datetime object.
    now = datetime.now(timezone.utc)
    connection = get_connection() 

    # 5.Stores the batch with the captured timestamp using the db.py helper
    with connection , connection.cursor() as cursor: 

        for ride in rides :
            ride_id = f'ride_{now:%Y%m%d%H%M%S}_{uuid4().hex}'
        
            # Here, %s is a placeholder interpreted by psycopg2, not Python string formatting.
            cursor.execute(
            "INSERT INTO raw.rides_stream VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [
                ride_id, 
                ride["driver_id"], 
                ride["pickup_zone_id"], 
                ride["dropoff_zone_id"], 
                ride["distance_km"], 
                ride["fare_amount"],
                ride["requested_at"]
            ]
            
            )

    cursor.close()

    print(f"Inserted {len(rides)} rides into raw.rides_stream.")



with DAG(
    dag_id="1_streaming",
    # Cron expression indicating Every minute Scheduling.
    schedule="* * * * *",          
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inDrive"],

) as dag:
    PythonOperator(
        task_id="pull_rides",
        python_callable=pull_rides,
    )
