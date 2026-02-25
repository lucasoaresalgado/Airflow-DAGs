from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
from mysql.connector import Error
import os

starrocks_host = os.getenv("STARROCKS_HOST")
starrocks_port = os.getenv("STARROCKS_PORT")
password_airflow = os.getenv("STARROCKS_AIRFLOW_PASSWORD")

def execute_query(query):
    try:
        conn = mysql.connector.connect(
            host= f'{starrocks_host}',
            port=starrocks_port,
            user='svc_airflow_writer',
            password=f'{password_airflow}',
            database='default_catalog.raw',
            auth_plugin='mysql_clear_password',
        )
        if conn.is_connected():
            print("Connection established successfully.")
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                conn.commit()
                print(f"Query executed: {query}")
            except Error as q_e:
                print(f"Error executing query: {query} -> {q_e}")
            cursor.close()
        else:
            print("Connection failed.")
    except Error as e:
        print(f"Error connecting: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

queries = {
    "TB_ACC_ACCESS_KEY": "INSERT OVERWRITE default_catalog.raw._crm_TB_ACC_ACCESS_KEY SELECT * FROM kaefer_bra_crm.dbo.TB_ACC_ACCESS_KEY;",
    "work_positions": "INSERT OVERWRITE default_catalog.raw._prjctsys_work_positions SELECT * FROM kaefer_bra_prjctsys.prjctsys.work_positions;",
    "employees": "INSERT OVERWRITE default_catalog.raw._prjctsys_employees SELECT * FROM kaefer_bra_prjctsys.prjctsys.employees;",
    "queueitem": "INSERT OVERWRITE default_catalog.raw._crm_queueitem SELECT * FROM default_catalog.externals.queueitem;",
    "quote": "INSERT OVERWRITE default_catalog.raw._crm_quote SELECT * FROM default_catalog.externals.quote;",
    "quotedetail": "INSERT OVERWRITE default_catalog.raw._crm_quotedetail SELECT * FROM default_catalog.externals.quotedetail;",
    "inspection_flexible_columns": "INSERT INTO default_catalog.raw._prjctsys_inspection_flexible_columns SELECT id, * EXCLUDE(id) FROM kaefer_bra_prjctsys.prjctsys.inspection_flexible_columns WHERE updated_at > seconds_add(now(), -259200);",
    "datasheet_flexible_columns": "INSERT INTO default_catalog.raw._prjctsys_datasheet_flexible_columns SELECT id, * EXCLUDE(id) FROM kaefer_bra_prjctsys.prjctsys.datasheet_flexible_columns WHERE updated_at > seconds_add(now(), -259200);",
}

with DAG(
    'star_k',
    default_args=default_args,
    description='StarRocks data extraction schedule k',
    schedule_interval="0 */3 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['lakehouse', 'api', 'starrocks']
) as dag:
    # Tasks for the smaller tables
    upstream_task_ids = [
        "TB_ACC_ACCESS_KEY",
        "work_positions",
        "employees",
        "queueitem",
        "quote",
        "quotedetail",
    ]
    upstream_tasks = []
    for task_id in upstream_task_ids:
        task = PythonOperator(
            task_id=f'execute_query_{task_id}',
            python_callable=execute_query,
            op_args=[queries[task_id]],
        )
        upstream_tasks.append(task)

    datasheet_flexible_columns_task = PythonOperator(
    task_id='execute_query_datasheet_flexible_columns',
    python_callable=execute_query,
    op_args=[queries["datasheet_flexible_columns"]],
    )
    inspection_flexible_columns_task = PythonOperator(
    task_id='execute_query_inspection_flexible_columns',
    python_callable=execute_query,
    op_args=[queries["inspection_flexible_columns"]],
    )

    for t in upstream_tasks:
        t >> [datasheet_flexible_columns_task, inspection_flexible_columns_task]