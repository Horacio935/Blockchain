import os
import requests

API_KEY = os.getenv("ETHERSCAN_API_KEY")
BASE_URL = "https://api.etherscan.io/v2/api"

address = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"  # Binance hot wallet (mucho tráfico)

params = {
    "chainid": 1,                 # 1 = Ethereum mainnet
    "module": "account",
    "action": "txlist",
    "address": address,
    "startblock": 0,
    "endblock": 99999999,
    "page": 1,
    "offset": 5,                  # 5 transacciones para probar
    "sort": "desc",
    "apikey": API_KEY
}

r = requests.get(BASE_URL, params=params, timeout=30)
data = r.json()
print("Status:", data.get("status"))
print("Message:", data.get("message"))
print("Ejemplo:", (data.get("result") or [None])[0])
