import logging
from datetime import datetime, timedelta
from typing import Dict

import requests
from time import sleep
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"


@dag(dag_id="dag-pipeline-3", schedule="@once", start_date=datetime(2023, 3, 25), catchup=False)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        context = get_current_context()
        # fallback si execution_date n'existe pas
        execution_date = context.get('execution_date', datetime.now())
        dates = []
        responses = []
        for n_day_before in range(0, 10):
            date = execution_date - timedelta(days=n_day_before)
            formatted_date = date.strftime("%d-%m-%Y")
            response = requests.get(API.format(formatted_date)).json()[
                "market_data"]
            dates.append(date)
            responses.append(response)
            sleep(6)
        return {"dates": dates, "responses": responses}

    @task(multiple_outputs=True)
    def process_data(extraction: Dict[str, float]) -> Dict[str, float]:
        logging.info(extraction)
        prices = [
            response["current_price"]["usd"]
            for response in extraction["responses"]
        ][::-1]
        print(prices)
        ups = [price for price in prices if price > 0]
        downs = [price for price in prices if price < 0]
        up_avg = sum(ups) / len(ups)
        down_avg = sum(downs) / len(downs)
        rsi = 100 - 100/(1 + up_avg/down_avg)
        return {
            "price": extraction["responses"][0]["current_price"]["usd"],
            "volume": extraction["responses"][0]["total_volume"]["usd"],
            "rsi": rsi,
        }

    @task
    def store_data(data: Dict[str, float]):
        logging.info(
            f"Store: Last price {data['price']} with volume {data['volume']} and rsi {data['rsi']}"
        )

    store_data(process_data(extract_bitcoin_price()))


taskflow()
