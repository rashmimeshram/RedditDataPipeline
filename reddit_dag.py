from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from reddit_etl import run_reddit_etl

dag_arg = {
    'owner': 'rashmi',
    'retries': '5',
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'reddit_dag',
    default_args = dag_arg,
    schedule = '@weekly',
    start_date=datetime(2024, 3, 6),
    catchup=True
)

run_etl = PythonOperator(
    task_id="task Complete",
    dag=dag,

)

run_etl
