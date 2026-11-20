"""
Dag pour obtenir le volume des fleurs, mais mal codé
"""

import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import dag, task


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id="flowers-dag",
    default_args=default_args,
    description="DAG sur les volumes",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],

)
def flower_dag():

    download_data = BashOperator(
        task_id="print_date",
        bash_command="curl https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv > /tmp/flower.csv",
    )

    @task
    def process_data():
        df = pd.read_csv("/tmp/flower.csv", sep=',')
        df["volume"] = df["sepal_length"] * df["sepal_width"]
        return len(df)

    longueur = process_data()

    @task.branch
    def decide_branch(longueur):
        if longueur <= 1000:
            return 'small'
        else:
            return 'big'

    branch_1 = decide_branch(longueur)

    @task(task_id="small")
    def small_dataframe():
        print("Petit dataframe")

    @task(task_id="big")
    def large_dataframe():
        print("Gros dataframe")

    download_data >> longueur >> branch_1
    branch_1 >> [small_dataframe(), large_dataframe()]


dag = flower_dag()
