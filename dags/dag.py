import sys
sys.path.append("/opt/airflow/project/")

import datetime as dt
from main import main

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {"owner": "alex", "start_date": dt.datetime(2023, 12, 24)}

with DAG(
    "alex_db_work",
    default_args=default_args,
    schedule_interval="@dayly",
) as dag:
    air_db = PythonOperator(task_id="analize_datas", python_callable=main())