from datetime import timedelta
from pendulum import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'bsp',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dag-catchup-is-false',
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    schedule='@daily',
    catchup=True,
) as dag:
    t1 = BashOperator(
        task_id="display",
        bash_command='echo toto',
    )
