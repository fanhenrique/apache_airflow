from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

@dag(start_date=datetime(2023, 6, 6), schedule=timedelta(minutes=1), catchup=False)
def create_tables():
  create_user_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'conn_postgres',
    sql = """
      CREATE TABLE IF NOT EXISTS user_table (
        "id" INTEGER PRIMARY KEY,
        "name" VARCHAR(100),
        "login" VARCHAR(100),
        "password" VARCHAR(100)
      );""",
  )
 

  
create_tables()