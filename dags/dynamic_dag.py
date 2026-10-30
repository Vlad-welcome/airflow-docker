from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
import time
import random

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dynamic_dag',
        default_args=default_args,
        description='dynamic maker tasks',
        schedule=None,
        tags=['dynamic']
):

    start = EmptyOperator(task_id='start')

    @task
    def get_data():
        return ["data_1", "data_2", "data_3"]

    @task
    def process_data(data: str):
        time.sleep(random.randint(1, 3))
        print(f"process with {data}")

    end = EmptyOperator(task_id='end')

    process_data_mapped = process_data.expand(data=get_data())

    start >> process_data_mapped >> end