import sys
sys.path.append("/opt/project/")

from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import main

default_args = {
   'owner': 'alex',
   'start_date': datetime(2023, 12, 24)
}

# для ежедневного запуска использовать schedule_interval="@daily"
dag = DAG('alex_work',
          schedule_interval="@once",
          default_args = default_args,
          catchup=True)

start_operator_01 = PythonOperator(task_id='work_with_db_2023-03-01',
                    python_callable=main, dag=dag)

start_operator_02 = PythonOperator(task_id='work_with_db_2023-03-02',
                    python_callable=main, dag=dag)

start_operator_03 = PythonOperator(task_id='work_with_db_2023-03-03',
                    python_callable=main, dag=dag)

start_operator_01 >> start_operator_02 >> start_operator_03