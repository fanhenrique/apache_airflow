import json

from airflow.decorators import dag, task

from datetime import datetime, timedelta

# ETL TaskFlow test
@dag(
  start_date=datetime(2023,6,1),
  schedule=timedelta(minutes=1),
  catchup=False,
  )
def etl_taskflow():

  @task()
  def extract():

    data = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    return json.loads(data)

  @task()
  def transform(data_dict: dict):
    data = data_dict.values()
    total_values = 0   
    for value in data:
      total_values += value

    return {'total_values': total_values}

  @task()
  def load(total_values: float):
    
    print(f'Total values: {total_values}')  

  data = extract()
  data_transformed = transform(data)
  load(data_transformed)

etl_taskflow()