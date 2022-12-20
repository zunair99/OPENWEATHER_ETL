from helpers.helper_funcs import load_to_postgres, join_to_worldcities
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Set default arguments
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Create DAG
with DAG(
    dag_id='openweather_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    join_to_worldcities = PythonOperator(
        task_id='join_to_worldcities',
        python_callable=join_to_worldcities,
        dag=dag,
    )