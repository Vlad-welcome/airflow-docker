from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='delay_task',
        default_args=default_args,
        description='delay',
        schedule=None,
        tags=['delay']
):

    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    start >> task_1() >> end


# from airflow.datasets import Dataset
# from airflow.decorators import dag, task
# from datetime import datetime

# # Простой датасет — например, путь к файлу или идентификатор
# my_dataset = Dataset("my_simple_data")

# @dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
# def producer_dag():
#     @task(outlets=[my_dataset])
#     def produce_data():
#         # Генерируем простую строку вместо реальных данных
#         data = "hello_world"
#         print(f"Produced: {data}")
#         return data  # Значение уйдёт в XCom

# producer_dag()