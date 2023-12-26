import sys
sys.path.append("/opt/project/")

from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import main

default_args = {
   'owner': 'alex',
   'start_date': datetime(2023, 12, 25)
}

# для запуска раз в день поставить schedule_interval="@daily"
dag = DAG('alex_work',
          schedule_interval="@daily",
          default_args = default_args,
          catchup=True)

start_operator = PythonOperator(task_id='work_with_db',
                    python_callable=main, dag=dag)