from airflow.decorators import task, dag
from datetime import datetime, timedelta

@dag(start_date=datetime(2023, 6, 1), schedule=timedelta(minutes=1), catchup=False)
def callable_virtualenv():
  
  @task.virtualenv(
    task_id="virtualenv_python", requirements=["colorama==0.4.0"], system_site_packages=False
  )
  def task_callable_virtualenv():

    from time import sleep

    from colorama import Back, Fore, Style

    print(Fore.RED + "some red text")
    print(Back.GREEN + "and with a green background")
    print(Style.DIM + "and in dim text")
    print(Style.RESET_ALL)
    for _ in range(4):
        print(Style.DIM + "Please wait...", flush=True)
        sleep(1)
    print("Finished")


  task_callable_virtualenv()

x = callable_virtualenv()