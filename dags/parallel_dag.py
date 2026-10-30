from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import time

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='parallel_dag',
        default_args=default_args,
        description='parallel',
        schedule=None,
        tags=['parallel']
):

    start = EmptyOperator(task_id='start')

    @task
    def data_db_task():
        time.sleep(1)
        print("data from DB")
        return "data from DB"

    @task
    def data_api_task():
        time.sleep(3)
        print("data from API")
        return "data from API"

    @task
    def merge_task(data_db, data_api):
        print(f"merge data: {data_db + data_api}")
        return data_db + data_api

    @task
    def send_task(data):
        print(f"send data: {data}")

    end = EmptyOperator(task_id='end')

    data_db = data_db_task()
    data_api = data_api_task()
    merge = merge_task(data_db, data_api)
    send = send_task(merge)

    start >> [data_db, data_api] >> merge >> send >> end