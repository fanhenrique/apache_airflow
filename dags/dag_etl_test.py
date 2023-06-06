import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def extract():

  data = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
  return json.loads(data)

def transform(ti):

  data = ti.xcom_pull(task_ids = 'extract')

  total_values = 0   
  for value in data.values():
    total_values += value

  return {'total_values': total_values}

def load(ti):
  
  total_values = ti.xcom_pull(task_ids = 'transform')
  print(f'Total values: {total_values}')

# ETL test
with DAG(
  'etl_test',
  start_date=datetime(2023,6,1),
  schedule=timedelta(minutes=1),
  catchup=False,
  ) as dag:

  task_extract = PythonOperator(
    task_id = 'extract',
    python_callable=extract
  )
  
  task_transform = PythonOperator(
    task_id = 'transform',
    python_callable=transform
  ) 
  
  task_load = PythonOperator(
    task_id = 'load',
    python_callable=load
  )

  task_extract >> task_transform >> task_load