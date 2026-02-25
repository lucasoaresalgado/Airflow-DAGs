from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import pyodbc
import logging

server = os.getenv("SERVER_PROTHEUS")
database = os.getenv("DATABASE_PRD")
user = os.getenv("USER_PRD_WRITE")
password = os.getenv("PASSWORD_PRD_WRITE")

# Função para executar a lógica do banco de dados
def execute_query():
    try:
        # Conexão com o banco de dados (use uma conexão configurada no Airflow)
        conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                              f'Server={server};'
                              f'Database={database};'
                              f'UID={user};'
                              f'PWD={password};'
                              'TrustServerCertificate=yes')
        cursor = conn.cursor()
        
        # Query SQL
        query = '''
        BEGIN TRANSACTION;

        -- Atualizar TM0010
        UPDATE TM0010 
        SET TM0010.TM0_MAT = SRA010.RA_MAT, 
            TM0010.TM0_FILFUN = SRA010.RA_FILIAL
        FROM TM0010 
        INNER JOIN SRA010 
            ON SRA010.D_E_L_E_T_ = '' 
            AND SRA010.RA_DEMISSA = '' -- Apenas colaboradores ativos
            AND TM0010.TM0_CPF = SRA010.RA_CIC
        WHERE TM0010.D_E_L_E_T_ = '' 
            AND TM0010.TM0_MAT = '' 
            AND TM0010.TM0_FILFUN = '';

        -- Atualizar TM0030
        UPDATE TM0030 
        SET TM0030.TM0_MAT = SRA030.RA_MAT, 
            TM0030.TM0_FILFUN = SRA030.RA_FILIAL
        FROM TM0030 
        INNER JOIN SRA030 
            ON SRA030.D_E_L_E_T_ = '' 
            AND SRA030.RA_DEMISSA = '' -- Apenas colaboradores ativos
            AND TM0030.TM0_CPF = SRA030.RA_CIC
        WHERE TM0030.D_E_L_E_T_ = '' 
            AND TM0030.TM0_MAT = '' 
            AND TM0030.TM0_FILFUN = '';

        IF @@ERROR <> 0
        BEGIN
            ROLLBACK TRANSACTION;
            PRINT 'Transaction Failed';
        END
        ELSE
        BEGIN
            COMMIT TRANSACTION;
            PRINT 'Transaction Successful';
        END
        '''
        
        cursor.execute(query)
        rows_deleted = cursor.rowcount  # Captura o número de linhas deletadas
        conn.commit()
        
        logging.info(f"Query executada com sucesso. {rows_deleted} linha(s) deletada(s).")
    except Exception as e:
        logging.error(f"Erro ao executar a query: {e}")
    finally:
        conn.close()

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'atualizar_ficha_medica',
    default_args=default_args,
    description='DAG para executar a rotina para atualizar filial + matricula na tabela de ficha medica ',
    schedule_interval="0 20 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    execute_query_task = PythonOperator(
        task_id='execute_query',
        python_callable=execute_query,
    )
    
    execute_query_task
