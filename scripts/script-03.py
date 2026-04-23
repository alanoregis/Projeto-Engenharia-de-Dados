import os
import sys
from pathlib import Path

# Garante que a raiz do projeto (onde está o pacote ingestión/) está no path,
# independente de onde o script é executado.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import dlt
import requests
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
    # Ao realizar o localmente, o MiniIO é acessível em localhost:9000.
    # Dentro do Docker, use http://minio:9000.
    load_dotenv()

    # Scripts locais sempre acessam o MINIO via localhost, mesmo que o .env
    # tenha http://minio:9000 (hostname válido apenas dentro do Docker)
    endpoint_url = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000").replace(
        "http://minio:", "http://localhost:"
    )

    destination = filesystem(
        bucket_url=os.environ.get("MINIO_BUCKET_URL", "s3://latest"),
        credentials={
            "aws_access_key_id": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            "aws_secret_access_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            "endpoint_url": endpoint_url,
        },
    )

    pipeline = dlt.pipeline(
        pipeline_name="freecurrency_pipeline",
        destination=destination,
        dataset_name="latest",
)
api_key = os.environ["API_KEY"]
load_info = pipeline.run(freecurrency_source(api_key=api_key), loader_file_format="parquet")
print(load_info)
