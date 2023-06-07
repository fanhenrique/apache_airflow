from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

@dag(
  start_date=datetime(2023, 6, 6),
  schedule=timedelta(minutes=1),
  catchup=False,
  template_searchpath='/opt/airflow/sql'  
)
def create_tables():
  
  create_user = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'conn_postgres',
    sql = 'create_user.sql',
  )
 
  insert_user = PostgresOperator(
    task_id = 'insert_user',
    postgres_conn_id = 'conn_postgres',
    sql = 'insert_user.sql',
  )

  create_user >> insert_user
  
create_tables()