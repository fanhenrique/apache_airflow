from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

with DAG(
  'bash_operator',
  start_date=datetime(2023,1,28),
  schedule=timedelta(minutes=5),
  catchup=False,
  ) as dag:

  #test task  
  bash_task_test = BashOperator(
    task_id = 'bash_task_test',
    bash_command = 'echo OK',
  )

  bash_task_test