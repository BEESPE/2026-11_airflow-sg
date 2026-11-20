import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import timedelta
from pendulum import datetime


@dag(
    dag_id="dag-branching-parameter-passing-new",
    start_date=datetime(2024, 3, 25),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "bsp",
        "retries": 4,
        "retry_delay": timedelta(minutes=1),
    },
)
def branching_tf_dag():

    # ----------------------------
    # Task 1: Download the dataset
    # ----------------------------
    download_data = BashOperator(
        task_id="download_data",
        bash_command=(
            "curl -o /tmp/flowerdataset.csv "
            "https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv"
        )
    )

    # ----------------------------
    # Task 2: Process data (TaskFlow)
    # ----------------------------
    @task
    def process_data():
        df = pd.read_csv('/tmp/flowerdataset.csv')

        # Compute new column
        df["somme"] = df["sepal_length"] + df["sepal_width"]
        print(df.head())

        # Return dataframe length
        return len(df)

    length = process_data()

    # ----------------------------
    # Task 3: Branching (TaskFlow)
    # ----------------------------
    @task.branch
    def branching(length: int):
        if length > 1000:
            return "large_dataset_branch"
        else:
            return "small_dataset_branch"

    branch = branching(length)

    # ----------------------------
    # Task 4: Branch tasks
    # ----------------------------
    @task(task_id="large_dataset_branch")
    def large_dataset():
        print("Gros dataset")

    @task(task_id="small_dataset_branch")
    def small_dataset():
        print("Petit dataset")

    # ----------------------------
    # Dependencies using TaskFlow syntax
    # ----------------------------
    download_data >> length >> branch
    branch >> [large_dataset(), small_dataset()]


# Instantiate the DAG
dag = branching_tf_dag()
