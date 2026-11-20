import json
import logging
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import pendulum
import requests
from airflow.sdk.exceptions import AirflowException, AirflowFailException
from airflow.sdk import Connection, Param, TaskGroup, Variable, dag, get_current_context, task
from coingecko_http import CoinGeckoHistoryOperator
from bitcoin_history import save_history

LOGGER = logging.getLogger(__name__)
BASE_URL = "https://api.coingecko.com/api/v3"
HISTORY_DAYS = 120


def filter_daily_response(response):
    """Réduire chaque réponse HTTP avant son stockage en XCom."""
    date = parse_qs(urlsplit(response.url).query)["date"][0]
    market = response.json().get("market_data") or {}
    price = market.get("current_price", {}).get("usd")
    volume = market.get("total_volume", {}).get("usd")
    if price is None or volume is None:
        raise AirflowException(f"Historique incomplet au {date} : prix ou volume USD absent.")
    result = {
        "date": pendulum.from_format(date, "DD-MM-YYYY", tz="UTC").to_date_string(),
        "market_data": {
            "current_price": {"usd": price},
            "total_volume": {"usd": volume},
        },
    }
    log_json("Réponse historique asynchrone :", result)
    return result


def log_json(message: str, data) -> None:
    """Afficher les données sur plusieurs lignes en conservant les accents."""
    LOGGER.info("%s\n%s", message, json.dumps(data, indent=2, ensure_ascii=False))


def get_coingecko(endpoint: str, params: dict) -> dict:
    """Lire la clé à l'exécution, sans la mettre dans les logs ni les XCom."""
    api_key = Variable.get("coin_gecko_api_key")
    response = requests.get(
        f"{BASE_URL}/{endpoint}",
        params=params,
        headers={"x-cg-demo-api-key": api_key},
        timeout=30,
    )
    LOGGER.info("CoinGecko GET /%s : HTTP %s", endpoint, response.status_code)
    if response.status_code == 429:
        raise AirflowException(
            "Rate limit CoinGecko (HTTP 429). Retry-After=%s. "
            "Relancer après le délai ; aucun remplacement automatique par du factice."
            % response.headers.get("Retry-After", "non fourni")
        )
    if not response.ok:
        raise AirflowException(
            f"CoinGecko HTTP {response.status_code} sur /{endpoint}. "
            "Vérifier la clé Demo, les droits et la date demandée."
        )
    return response.json()


def calculate_rsi(prices: list[float]) -> float:
    """RSI simple sur les variations fournies, sans lissage de Wilder."""
    if len(prices) < 2:
        raise ValueError("Le RSI nécessite au moins deux prix.")
    changes = [current - previous for previous, current in zip(prices, prices[1:])]
    average_gain = sum(max(change, 0) for change in changes) / len(changes)
    average_loss = sum(max(-change, 0) for change in changes) / len(changes)
    if average_loss == 0:
        return 50.0 if average_gain == 0 else 100.0
    return round(100 - 100 / (1 + average_gain / average_loss), 2)


def extract_real_history(execution_date, etape=4) -> list[dict]:
    """Obtenir dix snapshots à minuit UTC en un appel, de J-9 à J inclus."""
    end = execution_date.start_of("day")
    days = [end.subtract(days=offset) for offset in range(9, -1, -1)]
    params = {
        "vs_currency": "usd",
        "from": days[0].int_timestamp - 1,
        "to": end.int_timestamp + 1,
        "interval": "daily",
    }
    log_json(f"Historique réel demandé — étape {etape} :", params)
    response = get_coingecko("coins/bitcoin/market_chart/range", params)
    prices = dict(response.get("prices", []))
    volumes = dict(response.get("total_volumes", []))
    data = []
    for day in days:
        timestamp = day.int_timestamp * 1000  # CoinGecko renvoie des millisecondes.
        if prices.get(timestamp) is None or volumes.get(timestamp) is None:
            raise AirflowException(
                f"Historique réel incomplet : prix ou volume absent au "
                f"{day.to_date_string()} à 00:00 UTC. "
                "Dix snapshots quotidiens sont requis pour le RSI ; "
                "vérifier la disponibilité des données et les limites du plan Demo."
            )
        data.append({
            "date": day.to_date_string(),
            "market_data": {
                "current_price": {"usd": prices[timestamp]},
                "total_volume": {"usd": volumes[timestamp]},
            },
        })
    log_json(f"Historique Bitcoin réel — étape {etape} :", data)
    return data


def extract_daily_history(execution_date) -> list[dict]:
    """Reconstruire 120 jours distincts via /history, arrêt au premier échec."""
    data = []
    for index, offset in enumerate(range(HISTORY_DAYS - 1, -1, -1), start=1):
        day = execution_date.subtract(days=offset)
        LOGGER.info(
            "Étape 5 — requête %s/%s : historique Bitcoin du %s",
            index, HISTORY_DAYS, day.to_date_string(),
        )
        try:
            response = get_coingecko(
                "coins/bitcoin/history",
                {"date": day.strftime("%d-%m-%Y"), "localization": "false"},
            )
        except AirflowException:
            LOGGER.error(
                "Étape 5 interrompue à la requête %s/%s, date %s ; "
                "%s jours récupérés. Aucun appel supplémentaire ni retry.",
                index, HISTORY_DAYS, day.to_date_string(), len(data),
            )
            raise
        market = response.get("market_data") or {}
        price = market.get("current_price", {}).get("usd")
        volume = market.get("total_volume", {}).get("usd")
        if price is None or volume is None:
            raise AirflowException(f"Historique incomplet au {day.to_date_string()} : prix ou volume USD absent.")
        data.append({
            "date": day.to_date_string(),
            "market_data": {
                "current_price": {"usd": price},
                "total_volume": {"usd": volume},
            },
        })
    LOGGER.info("Étape 5 : %s jours récupérés sans HTTP 429 ; limite non atteinte.", len(data))
    log_json("Historique Bitcoin quotidien réel — étape 5 :", data)
    return data


@dag(
    dag_id="bitcoin_coingecko",
    description="TP Bitcoin : prix actuel, historique, RSI factice ou réel",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=64,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=2)},
    params={
        "etape": Param(
            1,
            type="integer",
            enum=[1, 2, 3, 4, 5, 6, 7],
            description="1 : prix ; 2 : historique ; 3 : RSI factice ; 4 : RSI réel ; 5 : rate limit ; 6 : PostgreSQL ; 7 : HTTP différable",
        ),
        "async_history_days": Param(
            120, type="integer", minimum=15, maximum=365,
            description="Mode 7 : nombre de jours et de tâches HTTP (15 à 365, limite Demo)",
        ),
    },
    tags=["tp", "bitcoin", "coingecko", "taskflow"],
    doc_md="""
    Trois tâches : `extract_bitcoin_price` → `process_data` → `store_data`.

    Déclencher manuellement avec le paramètre **etape** :
    - **1** (défaut) : prix actuel réel, résultat `{usd, change}` (variation en % sur 24 h).
    - **2** : historique réel à la date logique UTC, résultat `{usd, volume}`.
      Sans date logique (déclenchement manuel Airflow 3), utiliser `dag_run.run_after`.
      La date CoinGecko est formatée `DD-MM-YYYY`. Aucun décalage silencieux vers J-1.
    - **3** : dix réponses historiques factices, de J-9 à J, résultat
      `{usd, volume, rsi}`. Dix prix donnent neuf variations pour le RSI simple.
      Aucune requête API ni lecture de clé pour cette étape.
    - **4** : dix snapshots réels de J-9 à J à minuit UTC via un seul appel
      `/coins/bitcoin/market_chart/range` avec `interval=daily`.
      Résultat `{usd, volume, rsi}` ; erreur si une date manque ou si l'API refuse.
    - **5** : reconstruction de 120 jours réels via `/history`, un appel par date,
      de J-119 à J. Calcul d'un RSI simple sur les 14 dernières variations.
      Maximum 120 appels séquentiels, arrêt immédiat au premier échec, sans retry.
      Une limite non atteinte est aussi un résultat valide : aucun appel superflu.

    - **6** : même extraction réelle et RSI que le mode 4, puis enregistrement PostgreSQL.
    - **7** : TaskGroup `requests_async`, une tâche HTTP différable par jour.
      120 jours par défaut, paramétrables via `async_history_days` (15 à 365).
      Dates vérifiées avant le mapping : uniquement dans la fenêtre Demo actuelle.
      Chaque tâche a 4 retries avec backoff exponentiel (base 5 s, plafond 1 min).
      Pool limité à 64 requêtes simultanées ; aucun retry interne caché.
      Après épuisement des retries, repli sur le snapshot PostgreSQL de la même date.
      Chaque réponse réelle réussie alimente le cache ; aucune donnée factice n'y est enregistrée.
      RSI simple sur 14 variations ; stockage simulé. Le triggerer est nécessaire.

    Variable requise aux étapes 1, 2, 4, 5, 6 et 7 : `coin_gecko_api_key` (clé Demo).
    Étapes 1 à 5 et 7 : stockage simulé dans les logs.
    Étape 6 : stockage PostgreSQL via `bitcoin_db`, une ligne par run,
    mise à jour en cas de réexécution de la tâche.
    Les retries automatiques sont activés uniquement sur les requêtes du mode 7.
    """,
)
def bitcoin_coingecko():
    @task.branch
    def choose_extraction():
        return (
            "requests_async.prepare_requests"
            if get_current_context()["params"]["etape"] == 7
            else "extract_bitcoin_price"
        )

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def extract_bitcoin_price():
        context = get_current_context()
        etape = context["params"]["etape"]
        if etape == 1:
            data = get_coingecko(
                "simple/price",
                {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
            )["bitcoin"]
            log_json("Prix Bitcoin extrait — étape 1 :", data)
            return data

        dag_run = context["dag_run"]
        execution_date = pendulum.instance(dag_run.logical_date or dag_run.run_after).in_timezone("UTC")
        LOGGER.info("Étape %s : date de référence UTC %s", etape, execution_date.to_date_string())
        if etape == 2:
            data = get_coingecko(
                "coins/bitcoin/history",
                {"date": execution_date.strftime("%d-%m-%Y"), "localization": "false"},
            )
            log_json("Historique Bitcoin extrait — étape 2 :", data)
            return data

        if etape in (4, 6):
            return extract_real_history(execution_date, etape)
        if etape == 5:
            return extract_daily_history(execution_date)

        LOGGER.info("DONNÉES FACTICES : dix jours de J-9 à J, aucun appel CoinGecko")
        prices = [68000, 68500, 68200, 69000, 68800, 69500, 70000, 69700, 70500, 71000]
        data = [
            {
                "date": (execution_date - timedelta(days=9 - index)).to_date_string(),
                "market_data": {
                    "current_price": {"usd": price},
                    "total_volume": {"usd": 20_000_000_000 + index * 100_000_000},
                },
            }
            for index, price in enumerate(prices)
        ]
        log_json("Historique Bitcoin factice — étape 3 :", data)
        return data

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def process_data(data):
        etape = get_current_context()["params"]["etape"]
        if etape == 1:
            result = {"usd": data["usd"], "change": data["usd_24h_change"]}
            log_json("Données traitées — étape 1 :", result)
            return result

        latest = data[-1] if etape in (3, 4, 5, 6, 7) else data
        market_data = latest.get("market_data")
        if not market_data:
            raise AirflowException(
                "CoinGecko n'a pas renvoyé de market_data pour cette date. "
                "Vérifier la disponibilité du jour et la fenêtre historique du plan Demo."
            )
        result = {
            "usd": market_data["current_price"]["usd"],
            "volume": market_data["total_volume"]["usd"],
        }
        if etape in (3, 4, 5, 6, 7):
            rsi_data = data[-15:] if etape in (5, 7) else data
            result["rsi"] = calculate_rsi(
                [day["market_data"]["current_price"]["usd"] for day in rsi_data]
            )
            if etape in (5, 7):
                result.update(history_days=len(data), rsi_period=14)
            if etape == 7:
                fallback_dates = [day["date"] for day in data if day.get("source") == "postgres"]
                result.update(
                    fallback_dates=fallback_dates,
                    source_counts={"api": len(data) - len(fallback_dates), "postgres": len(fallback_dates)},
                )
        log_json(f"Données traitées — étape {etape} :", result)
        return result

    @task(show_return_value_in_logs=False)
    def store_data(data):
        context = get_current_context()
        etape = context["params"]["etape"]
        if etape != 6:
            log_json(f"Stockage simulé — étape {etape} :", data)
            return

        import psycopg2
        from psycopg2.extras import Json

        dag_run = context["dag_run"]
        connection = Connection.get("bitcoin_db")
        # Le contexte psycopg2 valide la transaction uniquement si elle réussit.
        db = psycopg2.connect(
            host=connection.host,
            port=connection.port or 5432,
            dbname=connection.schema,
            user=connection.login,
            password=connection.password,
            connect_timeout=10,
            options="-c statement_timeout=30000",
        )
        try:
            with db:
                with db.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO bitcoin_results (
                            dag_id, run_id, etape, execution_date, is_mock,
                            usd, volume, change, rsi, payload
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dag_id, run_id) DO UPDATE SET
                            etape = EXCLUDED.etape,
                            execution_date = EXCLUDED.execution_date,
                            is_mock = EXCLUDED.is_mock,
                            usd = EXCLUDED.usd,
                            volume = EXCLUDED.volume,
                            change = EXCLUDED.change,
                            rsi = EXCLUDED.rsi,
                            payload = EXCLUDED.payload,
                            updated_at = NOW()
                        """,
                        (
                            dag_run.dag_id, dag_run.run_id, etape,
                            dag_run.logical_date or dag_run.run_after, False,
                            data["usd"], data.get("volume"), data.get("change"),
                            data.get("rsi"), Json(data),
                        ),
                    )
                # Le mode 6 conserve aussi les dix snapshots pour les prochains replis.
                save_history(context["ti"].xcom_pull(task_ids="selected_history"), db=db)
        finally:
            db.close()
        log_json(f"Résultat enregistré dans bitcoin_results — étape {etape} :", data)

    with TaskGroup(group_id="requests_async", tooltip="Une requête différable par jour") as async_group:
        @task(show_return_value_in_logs=False)
        def prepare_requests():
            context = get_current_context()
            dag_run = context["dag_run"]
            day = pendulum.instance(dag_run.logical_date or dag_run.run_after).in_timezone("UTC")
            count = context["params"]["async_history_days"]
            today = pendulum.now("UTC").start_of("day")
            oldest = day.start_of("day").subtract(days=count - 1)
            # J à J-364 : éviter aussi la limite glissante au milieu de J-365.
            earliest = today.subtract(days=364)
            if oldest < earliest or day.start_of("day") > today:
                raise AirflowFailException(
                    f"Fenêtre CoinGecko Demo invalide : demande du {oldest.to_date_string()} "
                    f"au {day.to_date_string()}, dates autorisées du {earliest.to_date_string()} "
                    f"au {today.to_date_string()}. Réduire async_history_days ou corriger la date du run. "
                    "Aucune requête HTTP créée."
                )
            return [
                {"date": day.subtract(days=offset).strftime("%d-%m-%Y"), "localization": "false"}
                for offset in range(count - 1, -1, -1)
            ]

        requests_by_day = CoinGeckoHistoryOperator.partial(
            task_id="fetch_history",
            http_conn_id="coingecko_api",
            endpoint="api/v3/coins/bitcoin/history",
            method="GET",
            headers={"x-cg-demo-api-key": "{{ var.value.coin_gecko_api_key }}"},
            extra_options={"timeout": 30},
            response_filter=filter_daily_response,
            log_response=False,
            deferrable=True,
            retries=4,
            retry_delay=timedelta(seconds=5),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=1),
            max_active_tis_per_dag=64,
            pool="coingecko_http",
        ).expand(data=prepare_requests())

        @task(show_return_value_in_logs=False)
        def collect_history(responses):
            data = sorted(list(responses), key=lambda item: item["date"])
            expected = get_current_context()["params"]["async_history_days"]
            if len(data) != expected or len({day["date"] for day in data}) != expected:
                raise AirflowException("Historique asynchrone incomplet ou dates dupliquées.")
            log_json("Historique réel assemblé — étape 7 :", data)
            return data

        async_history = collect_history(requests_by_day.output)

    @task(trigger_rule="none_failed_min_one_success", show_return_value_in_logs=False)
    def selected_history():
        context = get_current_context()
        source = "requests_async.collect_history" if context["params"]["etape"] == 7 else "extract_bitcoin_price"
        return context["ti"].xcom_pull(task_ids=source)

    selected = choose_extraction()
    direct_history = extract_bitcoin_price()
    selected >> [direct_history, async_group]
    history = selected_history()
    [direct_history, async_history] >> history
    store_data(process_data(history))


bitcoin_coingecko()