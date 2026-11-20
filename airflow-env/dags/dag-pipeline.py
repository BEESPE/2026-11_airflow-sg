import logging
import requests

from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


@dag(
    dag_id="dag-pipeline",
)
def pipeline():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price():
        return requests.get(API).json()["bitcoin"]

    @task(multiple_outputs=True)
    def process_data(response):
        return {
            "usd": response.get("usd"),
            "change": response.get("usd_24h_change"),
        }

    @task
    def store_data(data):
        logging.info(f"J'ai stocké la valeur {data["usd"]} et le change {data["change"]}")

    store_data(
        process_data(
            extract_bitcoin_price()
        )
    )


dag = pipeline()
