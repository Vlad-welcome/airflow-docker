from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import mimetypes

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
    def send_text_task():
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

    @task
    def send_file_task():
        # Получаем значения переменных окружения
        smtp_host = os.getenv('AIRFLOW__SMTP__SMTP_HOST')
        smtp_port = os.getenv('AIRFLOW__SMTP__SMTP_PORT')
        smtp_user = os.getenv('AIRFLOW__SMTP__SMTP_USER')
        smtp_password = os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD')
        smtp_from = os.getenv('AIRFLOW__SMTP__SMTP_MAIL_FROM')

        file_path = "/opt/airflow/charts/report.docx"
        file_path2 = "/opt/airflow/charts/img.png"
        file_path3 = "/opt/airflow/charts/excel.xlsx"

        for p in (file_path, file_path2):
            if not os.path.isfile(p):
                raise FileNotFoundError(f"Файл не найден: {p}")

        msg = MIMEMultipart()
        msg['From'] = smtp_from
        msg["To"] = "vladtest.vladtest@yandex.ru"
        msg['Subject'] = "Письмо с вложением"
        body = "Привет. Основной текст"

        msg.attach(MIMEText(body, 'plain'))

        def attach_file(path: str):
            filename = os.path.basename(path)
            mime_type, _ = mimetypes.guess_type(path)
            if mime_type is None:
                mime_type = "application/octet-stream"
            maintype, subtype = mime_type.split("/", 1)

            with open(path, "rb") as f:               # ← обязательно "rb"
                part = MIMEBase(maintype, subtype)
                part.set_payload(f.read())
                encoders.encode_base64(part)               # ← base64 обязателен
                part.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=filename,
            )
            msg.attach(part)

        attach_file(file_path)
        attach_file(file_path2)
        attach_file(file_path3)

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

    start >> send_text_task() >> send_file_task() >> end