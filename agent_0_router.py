import os
import time
import json
import requests
import traceback
import google.generativeai as genai
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# --- Configuração Inicial ---
load_dotenv()
print("\n🔥 --- AGENTE 0: O PORTEIRO (Versão V3 - Inteligente) ---")

# 1. Carrega Variáveis
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_AGENT_1 = "topic-discovery-input"
# Tenta usar o modelo novo, mas o código tem fallback
MODELO_PREFERIDO = "gemini-2.0-flash-lite-preview-02-05" 

# 2. Configura Google
if not GEMINI_API_KEY:
    print("❌ ERRO: GEMINI_API_KEY não encontrada no .env")
    exit()

try:
    genai.configure(api_key=GEMINI_API_KEY)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_AGENT_1)
    print("✅ Pub/Sub configurado.")
except Exception as e:
    print(f"❌ Erro config: {e}")

last_update_id = 0

def get_telegram_updates(offset=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {"timeout": 10, "offset": offset}
    try:
        response = requests.get(url, params=params)
        return response.json()
    except:
        return {}

def send_telegram_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": str(text)}
    requests.post(url, json=payload)

def classify_intent(user_text):
    try:
        model = genai.GenerativeModel(MODELO_PREFERIDO)
    except:
        model = genai.GenerativeModel('gemini-1.5-flash')

    print(f"🧠 Analisando: '{user_text}'...")
    
    # --- PROMPT V3: CORRIGIDO PARA ACEITAR RESPOSTAS CURTAS ---
    prompt = f"""
    Você é um assistente de vendas. Classifique a entrada do usuário: "{user_text}"
    
    REGRA DE OURO:
    Se a entrada for um NICHO, SETOR ou LOCAL (ex: "Tecnologia", "Padarias", "Startups na Bahia", "Logística"), considere IMEDIATAMENTE como "SEARCH". O usuário provavelmente está respondendo uma pergunta anterior sua.
    
    CRITÉRIOS PARA BUSCA (SEARCH):
    1. Frases completas: "Quero empresas de TI", "Buscar fazendas".
    2. Palavras-chave soltas (Respostas): "Tecnologia", "Consultórios", "Bahia".
    
    CRITÉRIOS PARA CHAT (CHAT):
    1. Apenas cumprimentos ("Oi", "Tudo bem").
    2. Perguntas vazias ("Rola prospectar?", "Quero leads") -> Nesses casos, pergunte o segmento.
    3. Matemática ou piadas.
    
    Retorne APENAS JSON:
    {{ "type": "SEARCH", "query": "termo limpo (ex: Empresas de Tecnologia)" }}
    OU
    {{ "type": "CHAT", "response": "sua resposta" }}
    """
    
    try:
        response = model.generate_content(prompt)
        text = response.text.replace('```json', '').replace('```', '').strip()
        data = json.loads(text)
        return data
    
    except Exception as e:
        print(f"⚠️ Erro IA: {e}")
        # Se der erro, assume que é chat para não travar
        return {"type": "CHAT", "response": "Não entendi. Pode repetir o nicho?"}

def main():
    global last_update_id
    print(f"\n🤖 Bot Agente 0 RODANDO! (Agora aceita palavras-chave)")
    
    while True:
        updates = get_telegram_updates(last_update_id + 1)
        
        for update in updates.get("result", []):
            last_update_id = update["update_id"]
            
            if "message" in update and "text" in update["message"]:
                chat_id = update["message"]["chat"]["id"]
                text = update["message"]["text"]
                print(f"\n📨 Mensagem: {text}")
                
                decision = classify_intent(text)
                tipo = decision.get('type', 'CHAT')
                print(f"🤔 Decisão: {tipo}")

                if tipo == 'SEARCH':
                    query = decision.get('query', text)
                    send_telegram_message(chat_id, f"🔍 Entendido! Buscando: {query}")
                    
                    # Manda para a fábrica (Agente 1)
                    payload = {"command": query, "chat_id": chat_id}
                    publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
                    print("🚀 Enviado para Pub/Sub!")
                
                else:
                    resposta = decision.get('response')
                    if not resposta: resposta = "Qual segmento você quer?"
                    send_telegram_message(chat_id, resposta)

        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bot parado.")
