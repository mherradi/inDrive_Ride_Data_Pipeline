""" Tiny helpers :

Purpose:
    All four DAGs need to connect to PostgreSQL and execute SQL commands.Without helpers, we might repeat the same database 
    connection code in every file. This makes maintenance harder.

"""
import os
import psycopg2


def get_connection():

    """ Returns a connection to the PostgreSQL database. """

    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=5432,
        database=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"]
    )



def run_sql_file(path) :
    """ Allowing Airflow to execute the SQL commands from the given file. """

    with open(f"/opt/airflow/sql/{path}", "r", encoding="utf-8") as f:
        sql = f.read()

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()