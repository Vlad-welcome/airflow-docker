from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

def print_logical_date(**context):
    dt = context["logical_date"]
    print(f"Logical date: {dt}")

def print_logical_date2(dt):
    print(f"DS: {dt}")

def combined(ds_value, **context):
    print_logical_date(**context)
    print_logical_date2(ds_value)

with DAG(
    dag_id="test_print_dag",
    start_date=pendulum.datetime(2026, 10, 13, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=2,
    tags=["example"],
) as dag:
    
    print_date_task = PythonOperator(
        task_id="print_logical_date",
        python_callable=combined,
        op_args = ["{{ds}}"]
    )