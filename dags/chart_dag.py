from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from airflow.decorators import task

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='chart_dag',
        default_args=default_args,
        description='chart',
        schedule=None,
        tags=['chart']
):

    start = EmptyOperator(task_id='start')

    @task
    def generate_sales_chart():
        data = [10, 15, 7, 12, 20] # Пример данных
        path = "/opt/airflow/charts/sales_chart.png"

        plt.figure(figsize=(10, 6))
        plt.plot(data)
        plt.title("Sales Trend")
        plt.savefig(path) # Сохраняем график в файл
        print(f"Chart saved to {path}")

    end = EmptyOperator(task_id='end')

    start >> generate_sales_chart() >> end