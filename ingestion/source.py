"""
dlt source para a FreeCurrency API — endpoint /latest.

A API key é lida via dlt.secrets, que mapeia automaticamente a
variável de ambiente SOURCES__FREECURRENCY__API_KEY.
"""

import dlt
import requests
from dlt.sources import DltResource
from typing import Iterator


BASE_URL = "https://api.freecurrencyapi.com/v1"

# Moedas alvo disponíveis no plano gratuito da FreeCurrency API
# ARS, CLP e COP não estão disponíveis no plano free
DEFAULT_CURRENCIES = "EUR,GBP,JPY,USD,MXN,CAD,AUD"


@dlt.source(name="freecurrency")
def freecurrency_source(
    api_key: str = dlt.secrets.value,
    base_currency: str = "BRL",
    currencies: str = DEFAULT_CURRENCIES,
) -> DltResource:
    """
    Source dlt que expõe as cotações mais recentes da FreeCurrency API.

    Args:
        api_key: Chave da API. Lida de SOURCES__FREECURRENCY__API_KEY.
        base_currency: Moeda base para as cotações (default USD).
        currencies: Moedas alvo separadas por vírgula.
    """
    return latest_rates(api_key=api_key, base_currency=base_currency, currencies=currencies)


@dlt.resource(name="latest_rates", write_disposition="append")
def latest_rates(
    api_key: str,
    base_currency: str,
    currencies: str,
) -> Iterator[dict]:
    """
    Busca as cotações mais recentes e produz um registro por moeda.

    Cada registro inclui base_currency, target_currency, rate e
    extracted_at para facilitar particionamento e consultas temporais.
    """
    from datetime import datetime, timezone

    response = requests.get(
        f"{BASE_URL}/latest",
        params={
            "apikey": api_key,
            "base_currency": base_currency,
            "currencies": currencies,
        },
        timeout=30,
        verify=False,  # desabilita verificação SSL (certificado corporativo PicPay)
    )
    response.raise_for_status()

    payload = response.json()
    data: dict = payload.get("data", {})
    extracted_at = datetime.now(timezone.utc).isoformat()

    for target_currency, rate in data.items():
        yield {
            "base_currency": base_currency,
            "target_currency": target_currency,
            "rate": rate,
            "extracted_at": extracted_at,
        }