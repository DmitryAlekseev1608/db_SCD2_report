import sys
sys.path.append("/opt/project/")

from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import main
from datetime import datetime

currentDay = datetime.now().day
currentMonth = datetime.now().month
currentYear = datetime.now().year

default_args = {
   'owner': 'alex',
   'start_date': datetime(currentYear, currentMonth, currentDay)
}

dag = DAG('alex_work',
          schedule_interval="@daily",
          default_args = default_args,
          catchup=True)

start_operator_01 = PythonOperator(task_id='work_with_db_01_day',
                    python_callable=main, dag=dag)

start_operator_02 = PythonOperator(task_id='work_with_db_02_day',
                    python_callable=main, dag=dag)

start_operator_03 = PythonOperator(task_id='work_with_db_03_day',
                    python_callable=main, dag=dag)

start_operator_01 >> start_operator_02 >> start_operator_03