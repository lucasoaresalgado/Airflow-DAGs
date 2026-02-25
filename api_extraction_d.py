from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

def execute_api_call():
    endpoint = 'https://api-connect.sample.com.br/extract/schedule/api_d'
    try:
        print('Request sent, awaiting response...')
        response = requests.post(endpoint)
        status = response.status_code
        if status != 200:
            print(f'Response {status}: Extraction failed')
        else:
            print(f'Response {status}: Operation has succeeded')
    except requests.exceptions.RequestException as e:
        print(f'Network error: {e}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'api_d',
    default_args=default_args,
    description='api call schedule d',
    schedule_interval='0 0,8,16 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['lakehouse','api','apiconnect']
) as dag:
    execute_task = PythonOperator(
        task_id='execute_api_call',
        python_callable=execute_api_call
    )

    execute_task
