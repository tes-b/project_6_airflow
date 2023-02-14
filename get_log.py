from airflow import DAG
from airflow.providers.http.operators import http
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

import gzip
import json
# import boto3

default_args = {
    'owner':'admin',
    'retries':1,
    'retry_delay':timedelta(seconds=30),
    'email_on_failure':False
}

with DAG(
    dag_id="dag_get_log",
    start_date=datetime(2023,1,1),
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    ) as dag:

    def check_response(response, **kwargs):
        print("PROCESS check_response")
        if response.status_code == 200:
            execution_date = kwargs.get('execution_date') + timedelta(hours=9)
            json_response = json.loads(response.text)
            print("JSON_DATA : ",json_response)
            
            filename = f"data/log_{str(execution_date)}.json.gz"
            save_response(json_response, filename)
            return True
        return False
    
    def save_response(result, filename):
        # 압축
        with gzip.open(filename, 'wb') as f:
            f.write(json.dumps(result).encode('utf-8'))

        # with open(file_name,"w") as f:
        #     f.write(str(result))

    def upload_to_s3( **kwargs) -> None:

        # load data
        s3_hook = S3Hook(aws_conn_id='aws_default')
        # print("S3_hook : ", s3_hook)
        execution_date = kwargs.get('execution_date') + timedelta(hours=9)
        filename = f"data/log_{str(execution_date)}.json.gz"
        bucket_name = 'whdf-cp2-bucket'
        s3_hook.load_file(filename,key=filename,bucket_name=bucket_name,replace=True)

    def print_complete():
        print("DAG complete!")

    

    task_get_log = http.SimpleHttpOperator(
        task_id="task_get_log",
        method="GET",
        # endpoint="http://3.38.47.74/log/api/",
        http_conn_id='django_localhost',
        endpoint="/log/api/",
        data={"datefrom": "2023-02-08T10:32:32.077Z"},
        response_check=check_response,
        # response_check=lambda response: response.json(),
        
        dag=dag,
    )

    task_load_to_s3 = PythonOperator(
        task_id="task_load_to_s3",
        python_callable = upload_to_s3,
        op_kwargs = {
            'filename' : f'data/log_data.txt', # 파일 위치
            # 'key' : 'data/log_data.txt', 
            # bucket_name에 지정한 버킷 내에 파일을 어떤경로에 어떤 이름으로 저장할지 지정해준다.
            # 'bucket_name' : 'whdf-cp2-bucket' # 업로드 할 버킷
        }
    )

task_get_log >> task_load_to_s3
