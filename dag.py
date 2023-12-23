import sys

sys.path.append("/opt/hadoop/airflow/dags/alex_crypto/")
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import compute.current
import compute.days

default_args = {"owner": "alex", "start_date": dt.datetime(2023, 12, 14)}

with DAG(
    "alex_crypt_current_price",
    default_args=default_args,
    dagrun_timeout=dt.timedelta(minutes=60),
    schedule_interval="*/5 * * * *",
) as dag:
    air_current = PythonOperator(task_id="current_best_in_telegram", python_callable=compute.current.main)