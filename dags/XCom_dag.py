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

with DAG(dag_id='XCom_dag',
        default_args=default_args,
        description='Передача данных между шагами',
        schedule=None,
        tags=['give data']
):

    start = EmptyOperator(task_id='start')

    @task
    def extract_task():
        data = {"users": 100, "orders": 45}
        print(f"Извлечено: {data}")
        return data # ← автоматически в XCom

    @task
    def transform_task(data): # ← значение пришло из предыдущей задачи
        result = {k: v * 2 for k, v in data.items()}
        print(f"Преобразовано: {result}")
        return result

    @task
    def load_task(data):
        print(f"Загружено: {data}")

    end = EmptyOperator(task_id='end')

    extracted = extract_task()
    transformed = transform_task(extracted)
    loaded = load_task(transformed)

    start >> extracted >> transformed >> loaded >> end