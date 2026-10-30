from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='group_dag',
        default_args=default_args,
        description='group',
        schedule=None,
        tags=['group']
):

    start = EmptyOperator(task_id='start')

    @task_group(group_id="math")
    def math_data():
        @task
        def task_1(): 
            num = random.randint(1, 10)
            print(f"math: {num}")
            return num

        @task
        def task_2():
            num = random.randint(1, 10)
            print(f"math: {num}")
            return num

        @task
        def sum(num1, num2):
            sum = num1 + num2
            print(f"sum: {sum}")

        num1 = task_1()
        num2 = task_2()
        finall = sum(num1, num2)

        [num1, num2] >> finall

    @task_group(group_id="text")
    def text_data():
        @task
        def task_1(): 
            text = "text 1"
            print(text)
            return text

        @task
        def task_2():
            text = "text 2"
            print(text)
            return text

        @task
        def sum(text1, text2):
            sum = f"Total: {text1}, {text2}"
            print(sum)

        text1 = task_1()
        text2 = task_2()
        finall = sum(text1, text2)

        [text1, text2] >> finall

    end = EmptyOperator(task_id='end')

    start >> math_data() >> text_data() >> end