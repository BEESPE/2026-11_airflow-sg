import datetime
import logging
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"


@dag(
    dag_id="dag-pipeline-suite",
)
def pipeline():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price():
        context = get_current_context()
        execution_date = context.get("execution_date", datetime.now())
        formatted_date = execution_date.strftime("%d-%m-%Y")
        return requests.get(API.format(formatted_date)).json()["market_data"]

     @task(multiple_outputs=True)
     def process_data(response):
         return {
             "price": response.get("current_price").get("usd"),
             "volume": response.get("total_volume").get("usd"),
         }

     @task
     def store_data(data):
         logging.info(f"J'ai stocké la valeur {data["price"]} et le change {data["volume"]}")

    store_data(
        process_data(
            extract_bitcoin_price()
        )
    )


dag = pipeline()
