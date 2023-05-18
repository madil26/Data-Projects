from airflow import DAG 
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import weather_etl

default_args = {
    'owner': 'ADIL',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
    
}

def getWeather():
    weather_etl()

with DAG(
    default_args=default_args,
    dag_id='first dag etl python operator',
    start_date=datetime(2023,05,15),
    schedule_interval='@daily'
    
) as dag: 
    task1 = PythonOperator(
        tsak_id = 'etl',
        python_callable=getWeather
    )
task1