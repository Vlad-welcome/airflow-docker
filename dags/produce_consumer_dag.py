from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.sdk import Asset
from airflow.sdk import Variable

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

my_dataset = Asset("my_simple_data")

@dag(
    dag_id='producer_dag',
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args
)
def producer_dag():
    start = EmptyOperator(task_id='start')

    @task(outlets=[my_dataset])
    def produce_data():
        data = "hello_world"
        print(f"Produced: {data}")
        Variable.set("my_shared_data", data)
        return data

    end = EmptyOperator(task_id='end')

    start >> produce_data() >> end

@dag(
    dag_id='consumer_dag',
    schedule=[my_dataset],
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args
)
def consumer_dag():
    start = EmptyOperator(task_id='start')

    @task
    def consume_data():
        pulled_data = Variable.get("my_shared_data", default=None)
        print(f"Data consumed after dataset update: {pulled_data}")

    end = EmptyOperator(task_id='end')

    start >> consume_data() >> end

producer_dag()
consumer_dag()