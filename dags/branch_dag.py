from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
import random

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='branch_dag',
        default_args=default_args,
        description='two branches',
        schedule=None,
        tags=['choose']
):

    start = EmptyOperator(task_id='start')

    @task.branch
    def choose_branch():
        if random.randint(0, 1) > 0:
            print("task_1")
            return "task_1"
        print("task_2")
        return "task_2"

    @task
    def task_1():
        print("task 1")

    @task
    def task_2():
        print("task 2")

    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("any time")

    end = EmptyOperator(task_id='end')

    start >> choose_branch() >> [task_1(), task_2()] >> finish() >> end