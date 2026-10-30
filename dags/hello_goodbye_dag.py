from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='hello_goodbye_dag',
        default_args=default_args,
        description='print hello, goodbye',
        start_date=datetime(2026,10,29),
        schedule='@daily',
        catchup=False,
        tags=['example123123']
):

    start = EmptyOperator(task_id='start')

    @task
    def print_hello_task():
        print('hello');

    @task
    def print_goodbye_task():
        print('goodbye');

    end = EmptyOperator(task_id='end')

    start >> print_hello_task() >> print_goodbye_task() >> end