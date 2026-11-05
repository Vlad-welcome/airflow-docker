from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import HttpOperator
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
def api():
    call_api = HttpOperator(
        task_id="get_api_json",
        http_conn_id="http_railway",
        # endpoint="jsonplaceholder.typicode.com/todos/1",
        endpoint="nodeapiairflow-production.up.railway.app/api/airflow/data",
        method="GET",
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    @task
    def proceess_response(response):
        print(f"The title is: {response['data']}")

    proceess_response(call_api.output)


api()
# get_api_data >> proceess_response


# def api_railway():
#     url = "https://nodeapiairflow-production.up.railway.app/api/airflow/data"
#     response = requests.get(url, timeout=10)

#     # Проверка статуса (выбросит исключение при 4xx/5xx)
#     response.raise_for_status()

#     # Парсинг JSON
#     data = response.json()
#     print(data)


# api_railway()
