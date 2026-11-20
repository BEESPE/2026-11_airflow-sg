import requests
import random
from datetime import timedelta
from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator


API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"


# -----------------------------------------------------
# 1. Hook API pour CoinGecko
# -----------------------------------------------------
class BitcoinAPIHook(BaseHook):

    conn_name_attr = "bitcoin_api_conn_id"

    def __init__(self, bitcoin_api_conn_id="bitcoin_api_default"):
        super().__init__()
        self.bitcoin_api_conn_id = bitcoin_api_conn_id

    def get_price_on_date(self, date_str: str) -> float:
        url = API.format(date_str)
        response = requests.get(url).json()
        return response["market_data"]["current_price"]["usd"]


# -----------------------------------------------------
# 2. Operator extraction bitcoin (Airflow 3)
# -----------------------------------------------------
class BitcoinExtractOperator(BaseOperator):

    def __init__(
        self,
        execution_date,
        bitcoin_api_conn_id="bitcoin_api_default",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.execution_date = execution_date
        self.bitcoin_api_conn_id = bitcoin_api_conn_id

    def execute(self, context):

        hook = BitcoinAPIHook(self.bitcoin_api_conn_id)

        # Date principale
        formatted_date = self.execution_date.strftime("%d-%m-%Y")
        initial_price = hook.get_price_on_date(formatted_date)

        dates = []
        prices = []

        # Historique avec variations aléatoires
        for n_day_before in range(0, 10):
            d = self.execution_date - timedelta(days=n_day_before)
            formatted = d.strftime("%d-%m-%Y")
            random_variation = round(random.uniform(-1000, 1000), 2)
            dates.append(d)
            prices.append(initial_price + random_variation)

        return {"dates": dates, "prices": prices}


# -----------------------------------------------------
# 3. Plugin Loader
# -----------------------------------------------------
class BitcoinPlugin(AirflowPlugin):
    name = "bitcoin_plugin"
    hooks = [BitcoinAPIHook]
    operators = [BitcoinExtractOperator]
