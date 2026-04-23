import requests
import os
from dotenv import load_dotenv

load_dotenv()
response = requests.get(
    "https://api.freecurrencyapi.com/v1/latest",
    params={
        "apikey": os.getenv("API_KEY"),
        "base_currency": "BRL",
        "currencies": "USD,EUR,JPY"
    }
)

print(response.status_code)
print(response.json())