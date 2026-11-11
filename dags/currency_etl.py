import os
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient, UpdateOne

try:                                    # Airflow 3
    from airflow.sdk import dag, task
except ImportError:                     # Airflow 2
    from airflow.decorators import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
QUOTES = "USD,GBP,JPY"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily ETL: Frankfurter API -> MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    # ---------- TASK 1: EXTRACT ----------
    @task
    def extract_exchange_rates(ti=None):
        response = requests.get(
            API_URL,
            params={"base": BASE_CURRENCY, "quotes": QUOTES},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        print("RAW API RESPONSE:", data)

        # Named XCom
        ti.xcom_push(key="raw_exchange_rates", value=data)

    # ---------- TASK 2: TRANSFORM ----------
    @task
    def transform_exchange_rates(ti=None):
        raw = ti.xcom_pull(
            task_ids="extract_exchange_rates", key="raw_exchange_rates"
        )

        # v2 returns a list of {date, base, quote, rate};
        # older format returns {date, base, rates:{...}}. Handle both.
        if isinstance(raw, list):
            items = raw
        else:
            items = [
                {"date": raw["date"], "base": raw["base"], "quote": q, "rate": r}
                for q, r in raw["rates"].items()
            ]

        retrieved_at = datetime.now().isoformat(timespec="seconds")
        transformed = [
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for item in items
        ]
        print("TRANSFORMED:", transformed)

        # Named XCom
        ti.xcom_push(key="transformed_exchange_rates", value=transformed)

    # ---------- TASK 3: LOAD ----------
    @task
    def load_to_mongodb(ti=None):
        records = ti.xcom_pull(
            task_ids="transform_exchange_rates", key="transformed_exchange_rates"
        )
        if not records:
            raise ValueError("No records received from transform task")

        client = MongoClient(os.environ["MONGO_URI"])
        collection = client[os.environ["MONGO_DB"]][os.environ["MONGO_COLLECTION"]]

        # Duplicate se bachne ke liye upsert (same din dobara run ho to duplicate nahi banega)
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
        )
        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)
        print(f"Inserted: {result.upserted_count}, Updated: {result.modified_count}")
        client.close()

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()