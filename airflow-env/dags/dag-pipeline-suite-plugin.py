import requests
from datetime import datetime
import logging
from datetime import datetime, timedelta

import numpy as np

from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from bitcoin_plugin import BitcoinExtractOperator


@dag(dag_id="dag-pipeline-suite-plugin", schedule="@once", start_date=datetime(2023, 3, 25), catchup=False)
def taskflow():
    @task
    def extract_bitcoin_price():
        context = get_current_context()
        execution_date = context.get('execution_date', datetime.now())

        op = BitcoinExtractOperator(
            task_id="extract_bitcoin_price",
            execution_date=execution_date,
        )
        op.execute(context={"ds": execution_date.strftime("%d-%m-%Y")})

    @task(multiple_outputs=True)
    def process_data(extraction):
        logging.info(extraction)
        prices = extraction["prices"][::-1]
        print(prices)
        deltas = np.diff(prices)
        print(deltas)
        ups = [delta for delta in deltas if delta > 0]
        downs = [-delta for delta in deltas if delta < 0]
        up_avg = sum(ups) / len(ups)
        print(up_avg)
        down_avg = sum(downs) / len(downs)
        print(down_avg)
        rsi = 100 - 100/(1 + up_avg/down_avg)
        return {
            "last_price": prices[-1],
            "rsi": rsi,
        }

    @task
    def store_data(data):
        logging.info(
            f"Store: Last price {data['last_price']} with rsi {data['rsi']}"
        )

    store_data(process_data(extract_bitcoin_price()))


taskflow()
