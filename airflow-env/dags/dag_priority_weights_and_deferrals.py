from datetime import datetime, timedelta
import logging
import time

from airflow.decorators import dag, task
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator

# Fonction Python simulée
def dummy_task_function(task_name):
    time.sleep(5)
    logging.info(f"Running task: {task_name}")

# ------------------------------
# Définition du DAG
# ------------------------------
@dag(
    dag_id="dag_priority_weights_and_deferrals_tf",
    description="DAG avec TimeDeltaSensor et priorité (sans SLA)",
    schedule="@daily",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["your_email@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def pipeline():

    # ------------------------------
    # Start task (remplace le DummyOperator)
    # ------------------------------
    @task
    def start_task():
        logging.info("DAG started")

    # ------------------------------
    # Wait task avec TimeDeltaSensor
    # ------------------------------
    wait = TimeDeltaSensor(
        task_id="wait_for_5_minutes",
        delta=timedelta(seconds=10)
    )

    # ------------------------------
    # Tâches avec poids de priorité
    # ------------------------------
    high_priority_task = PythonOperator(
        task_id='high_priority_task',
        python_callable=dummy_task_function,
        op_args=['high_priority_task'],
        priority_weight=10,
    )

    low_priority_task = PythonOperator(
        task_id='low_priority_task',
        python_callable=dummy_task_function,
        op_args=['low_priority_task'],
        priority_weight=1,
    )

    # ------------------------------
    # End task (remplace le DummyOperator)
    # ------------------------------
    @task
    def end_task():
        logging.info("DAG finished")

    # ------------------------------
    # Dépendances
    # ------------------------------
    start = start_task()
    end = end_task()

    start >> wait >> [high_priority_task, low_priority_task] >> end

# Instanciation du DAG
pipeline()
