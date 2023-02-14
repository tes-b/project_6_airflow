from airflow import DAG
from airflow.providers.http.operators import http
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import json
import requests

default_args = {
    'owner':'admin',
    'retries':1,
    'retry_delay':timedelta(seconds=30),
    'email_on_failure': False
}

with DAG(
    dag_id="dag_create_dummy",
    start_date=datetime(2023,1,1),
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    ) as dag:

    def check_login_response(response, **context):
        print("PROCESS check_response")
        if response.status_code == 200:
            json_response = json.loads(response.text)
            print("RESPONSE : ", json_response)
            task_instance = context['task_instance']
            task_instance.xcom_push(key='login_response', value=json_response)
            return True
        return False

    task_login = http.SimpleHttpOperator(
        task_id="task_login",
        method="POST",
        http_conn_id='django_localhost',
        endpoint="/accounts/api/login/",
        data={
            "username": "asdf",
            "password": "asdf"
            },
        response_check=check_login_response,
        dag=dag,
    )    

    def write_board(**context):
        task_instance = context['task_instance']
        login_response = task_instance.xcom_pull(key='login_response')
        print("RESPONSE from xcom : ", login_response)

        url = "http://127.0.0.1:8000/board/api/create/"

        headers = {
            'Content-Type': 'application/json', 
            'charset': 'UTF-8', 
            'Accept': '*/*',
            'Authorization': f"Bearer {login_response['token']['access']}"
            }

        body = {
            "title": "dummy",
            "content": "content",
            "author_id": f"{login_response['user_id']}",
            }
        for _ in range(2):
            response = requests.post(url, headers=headers, data=json.dumps(
                body, ensure_ascii=False, indent="\t"))

        

    task_write_board = PythonOperator(
        task_id="task_write_board",
        python_callable = write_board,
        dag=dag
    )

task_login >> task_write_board