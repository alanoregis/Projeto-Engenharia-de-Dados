import dlt
import requests
import os
from dotenv import load_dotenv
from typing import Iterator
from dlt.sources import DltResource
from dlt.destinations import filesystem
from datetime import datetime, timezone

BASE_URL = "https://api.freecurrencyapi.com/v1"
DEFAULT_CURRENCIES = "EUR,JPY,USD"

@dlt.resource(name="latest_rates", write_disposition="append")
def latest_rates(api_key: str, base_currency: str, currencies: str) -> Iterator[dict]:
    response = requests.get(
        f"{BASE_URL}/latest",
        params={"apikey": api_key, "base_currency": base_currency, "currencies": currencies},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json().get("data", {})
    extracted_at = datetime.now(timezone.utc).isoformat()

    for target_currency, rate in data.items():
        yield {
            "base_currency": base_currency,
            "target_currency": target_currency,
            "rate": rate,
            "extracted_at": extracted_at,
        }

@dlt.source(name="freecurrency")
def freecurrency_source(
    api_key: str,
    base_currency: str = "BRL",
    currencies: str = DEFAULT_CURRENCIES,
) -> DltResource:
    return latest_rates(api_key=api_key, base_currency=base_currency, currencies=currencies)

if __name__ == "__main__":
    load_dotenv()
    api_key = os.environ["API_KEY"]

    destination = filesystem(bucket_url="data")

    pipeline_local = dlt.pipeline(
        pipeline_name="freecurrency_pipeline",
        destination=destination,
        dataset_name="latest",
    )
    load_info = pipeline_local.run(freecurrency_source(api_key=api_key))
    print(load_info)