import logging
from datetime import datetime
from typing import Dict

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"


@dag(dag_id="dag-pipeline-2", schedule="@once", start_date=datetime(2023, 3, 25), catchup=False)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        context = get_current_context()
        # fallback si execution_date n'existe pas
        execution_date = context.get('execution_date', datetime.now())
        formatted_date = execution_date.strftime("%d-%m-%Y")
        return requests.get(API.format(formatted_date)).json()["market_data"]

    @task(multiple_outputs=True)
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {
            "price": response["current_price"]["usd"],
            "volume": response["total_volume"]["usd"],
        }

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['price']} with volume {data['volume']}")

    store_data(process_data(extract_bitcoin_price()))


taskflow()
