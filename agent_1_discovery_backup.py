
import os
import json
import time
import requests
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# --- Configuração ---
load_dotenv()
print("\n🕵️ --- AGENTE 1: DISCOVERY (Modo Debug de Erro) ---")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
SUBSCRIPTION_NAME = "sub-telegram-input" 
NEXT_TOPIC_NAME = "topic-tech-filter"    
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") 

if not PERPLEXITY_API_KEY:
    print("❌ ERRO: PERPLEXITY_API_KEY não configurada!")
    exit()

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, NEXT_TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

def notify_telegram(chat_id, text):
    if not chat_id: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text})

def search_perplexity(query):
    print(f"🔎 Consultando Perplexity: '{query}'...")
    
    url = "https://api.perplexity.ai/chat/completions"
    payload = {
        # Vamos usar um modelo genérico para testar se o problema é o nome do modelo
        "model": "sonar", 
        "messages": [
            {"role": "system", "content": "Retorne APENAS JSON com domínios. Ex: {'domains': ['site.com']}."},
            {"role": "user", "content": f"Voce esta ajudando no suporte para prospectar clientes procure 20 sites corporativos para: {query}"}
        ]
    }
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        # --- AQUI ESTA A MUDANÇA PARA DESCOBRIR O ERRO ---
        if response.status_code != 200:
            print(f"⚠️ ERRO HTTP {response.status_code}!")
            print(f"📜 RESPOSTA DA API: {response.text}") # Isso vai nos dizer o motivo!
            return []

        content = response.json()['choices'][0]['message']['content']
        
        # Limpeza JSON
        start = content.find('{')
        end = content.rfind('}') + 1
        json_str = content[start:end]
        data = json.loads(json_str)
        
        domains = [d.replace("https://", "").replace("www.", "").strip("/") for d in data.get('domains', [])]
        return domains

    except Exception as e:
        print(f"❌ Erro Técnico: {e}")
        return []

def callback(message):
    try:
        print(f"\n📨 Job recebido...")
        data = json.loads(message.data.decode("utf-8"))
        command = data.get("command")
        chat_id = data.get("chat_id")
        
        domains = search_perplexity(command)
        
        if not domains:
            print("⚠️ Falha na busca.")
            notify_telegram(chat_id, f"⚠️ Erro ao buscar '{command}'. Verifique o terminal da VM.")
            message.ack()
            return

        msg = f"✅ Encontrei {len(domains)} empresas para '{command}':\n" + "\n".join(domains)
        notify_telegram(chat_id, msg)

        for domain in domains:
            payload = {"domain": domain, "origin_query": command, "chat_id": chat_id}
            publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))

        message.ack()
        
    except Exception as e:
        print(f"🔥 Erro: {e}")
        message.nack()

if __name__ == "__main__":
    print(f"🎧 Agente 1 ouvindo fila...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback).result()
        except KeyboardInterrupt: pass
