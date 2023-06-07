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
        ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        name VARCHAR(100),
        date date
      );""",
  )
 
  insert_user = PostgresOperator(
    task_id = 'insert_user',
    postgres_conn_id = 'conn_postgres',
    sql = """
      INSERT INTO user_table (name, date)
      VALUES ('joao', CURRENT_DATE);
      """,
  )

  create_user_table >> insert_user
  
create_tables()