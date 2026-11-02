from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

dag_owner = 'vlad'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='email_yandex_dag',
        default_args=default_args,
        description='email',
        schedule=None,
        tags=['email']
):
    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        # Получаем значения переменных окружения
        smtp_host = os.getenv('AIRFLOW__SMTP__SMTP_HOST')
        smtp_port = os.getenv('AIRFLOW__SMTP__SMTP_PORT')
        smtp_user = os.getenv('AIRFLOW__SMTP__SMTP_USER')
        smtp_password = os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD')
        smtp_from = os.getenv('AIRFLOW__SMTP__SMTP_MAIL_FROM')

        msg = MIMEMultipart()
        msg['From'] = smtp_from
        msg["To"] = "vladtest.vladtest@yandex.ru"
        msg['Subject'] = "Заглавие сообщения"
        body = "Привет. Основной текст"

        msg.attach(MIMEText(body, 'plain'))

        try:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
            server.login(smtp_user, smtp_password)

            server.sendmail(smtp_from, msg["To"], msg.as_string())
            server.quit()
            print(f"Отправлено: {smtp_from}")
        except Exception as e:
            print(f"Ошибка: {e}")
            raise ValueError("email_on_failure")

    end = EmptyOperator(task_id='end')

    start >> task_1() >> end