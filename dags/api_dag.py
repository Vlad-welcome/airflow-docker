from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests

dag_owner = "vlad"

default_args = {
    "owner": dag_owner,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_dag",
    default_args=default_args,
    description="api",
    schedule=None,
    tags=["api"],
)
def api_dag():

    @task
    def start():
        print("start")

    @task
    def api_railway():
        url = "http://host.docker.internal:3000/api/airflow/data"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(data)
        return data

    @task
    def end():
        print("end")

    start() >> api_railway() >> end()


api_dag()
