import os
import requests
from dotenv import load_dotenv

load_dotenv()
LUSHA_KEY = os.getenv("LUSHA_API_KEY")

print(f"\n🔑 Testando chave: {LUSHA_KEY[:5]}...[oculto]")

url = "https://api.lusha.com/person/enrich"
headers = {"api_key": LUSHA_KEY}
# Teste com uma empresa fácil (Facebook)
params = {"domain": "facebook.com", "jobTitle": "Marketing"}

try:
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    print(f"📡 Status Code: {resp.status_code}")
    print(f"📜 Resposta: {resp.text}")
except Exception as e:
    print(f"❌ Erro de conexão: {e}")
