from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from docxtpl import DocxTemplate
from xltpl.writerx import BookWriter

dag_owner = "vlad"

default_args = {
    "owner": dag_owner,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="template_docx_dag.py",
    default_args=default_args,
    description="docx",
    schedule=None,
    tags=["docx"],
):

    start = EmptyOperator(task_id="start")

    @task
    def docx_task():
        doc = DocxTemplate("/opt/airflow/reports/template.docx")
        context = {
            "name": "«Ромашка»",
            "array": ["item1", "item2", "item3"],
        }
        doc.render(context)
        doc.save("/opt/airflow/reports/report_T.docx")

    @task
    def excel_task():
        excel = BookWriter("/opt/airflow/reports/template.xlsx")
        context = {
            "name": "«Ромашка»",
            "array": ["item1", "item2", "item3"],
        }
        excel.render_book([context])
        excel.save("/opt/airflow/reports/report_T.xlsx")

    end = EmptyOperator(task_id="end")

    start >> docx_task() >> excel_task() >> end
