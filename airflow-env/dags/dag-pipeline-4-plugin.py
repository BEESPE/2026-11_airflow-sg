import logging
import numpy as np
from datetime import datetime
from typing import Dict

from airflow.decorators import dag, task
from bitcoin_plugin import BitcoinExtractOperator

# ------------------------------
# DAG
# ------------------------------
@dag(
    dag_id="dag_pipeline_plugin_taskflow",
    schedule="@once",
    start_date=datetime(2023, 3, 25),
    catchup=False,
)
def pipeline():

    # ------------------------------
    # TaskFlow wrapper pour utiliser l'Operator du plugin
    # ------------------------------
    @task
    def extract_bitcoin_task(execution_date: datetime) -> Dict[str, float]:
        """
        Wrapper TaskFlow autour du plugin BitcoinExtractOperator.
        Renvoie directement le dictionnaire avec dates et prices.
        """
        op = BitcoinExtractOperator(
            task_id="extract_bitcoin_price",
            execution_date=execution_date
        )
        # On exécute l'Operator manuellement pour récupérer le résultat
        return op.execute(context={"ds": execution_date.strftime("%Y-%m-%d")})

    # ------------------------------
    # Traitement des données
    # ------------------------------
    @task(multiple_outputs=True)
    def process_data(extraction: Dict[str, float]) -> Dict[str, float]:
        prices = extraction["prices"][::-1]
        deltas = np.diff(prices)

        ups = [d for d in deltas if d > 0]
        downs = [-d for d in deltas if d < 0]

        up_avg = sum(ups) / len(ups) if ups else 0.0
        down_avg = sum(downs) / len(downs) if downs else 0.0

        # RSI simple
        rsi = 100 - 100 / (1 + up_avg / down_avg) if down_avg != 0 else 100

        logging.info(f"Prices: {prices}")
        logging.info(f"RSI: {rsi}")

        return {"last_price": prices[-1], "rsi": rsi}

    # ------------------------------
    # Stockage / affichage
    # ------------------------------
    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: Last price {data['last_price']} with RSI {data['rsi']}")

    # ------------------------------
    # Pipeline
    # ------------------------------
    extraction = extract_bitcoin_task(datetime.now())
    store_data(process_data(extraction))


# Instanciation du DAG
pipeline()
