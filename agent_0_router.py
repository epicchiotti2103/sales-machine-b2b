
import os
import time
import json
import requests
import google.generativeai as genai
from google.cloud import pubsub_v1
from google.cloud import firestore
from dotenv import load_dotenv

# --- Configuração Inicial ---
load_dotenv()
print("\n🔥 --- AGENTE 0: O PORTEIRO (V5.2 - Híbrido Estável) ---")

# 1. Carrega Variáveis
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# Tópicos
TOPIC_AGENT_1 = "topic-discovery-input"   # Busca Original (Texto)
TOPIC_AGENT_3 = "topic-enricher"          # NOVO: Para mandar o comando do botão

# Carrega lista de usuários permitidos
ALLOWED_USERS_RAW = os.getenv("ALLOWED_USERS", "")
ALLOWED_USERS = [id.strip() for id in ALLOWED_USERS_RAW.split(",") if id.strip()]

# Modelo mantido
MODELO_PREFERIDO = "gemini-2.0-flash-lite-preview-02-05" 

# --- MEMÓRIA VOLÁTIL ---
user_histories = {}
HISTORY_LIMIT = 6

# --- SEU TEMPLATE DE BUSCA ORIGINAL (Restaurado) ---
TEMPLATE_BUSCA = """
Contexto: Empresa de mídia digital com atuação em Jobs e Marketing Mobile.

Objetivo: Prospectar {pedido}

Formato de Resposta JSON:
{{
"prospecting_request": "{pedido}",
"companies": [
{{
"name": "Nome",
"sector": "Setor",
"location": "Localização",
"size": "Porte (P/M/G)",
"relevance_score": "1-10",
"website": "URL do site (obrigatório)",
"contact_points": "LinkedIn/Email Geral",
"fit_explanation": "Por que é relevante"
}}
],
"market_insights": "Insights curtos do segmento",
"next_actions": ["ação 1", "ação 2"]
}}
"""

# 2. Configura Google
if not GEMINI_API_KEY:
    print("❌ ERRO: GEMINI_API_KEY não encontrada no .env")
    exit()

if not ALLOWED_USERS:
    print("⚠️ AVISO: Nenhuma lista de ALLOWED_USERS configurada.")

try:
    genai.configure(api_key=GEMINI_API_KEY)
    publisher = pubsub_v1.PublisherClient()
    
    # Tópico 1 (Busca)
    topic_path_1 = publisher.topic_path(PROJECT_ID, TOPIC_AGENT_1)
    # Tópico 3 (Enriquecimento via Botão)
    topic_path_3 = publisher.topic_path(PROJECT_ID, TOPIC_AGENT_3)
    
    # Firestore (Para descartar leads direto no banco)
    db = firestore.Client(project=PROJECT_ID)
    
    print("✅ Pub/Sub e Firestore configurados.")
except Exception as e:
    print(f"❌ Erro config: {e}")

last_update_id = 0

# --- FUNÇÕES TELEGRAM ---

def get_telegram_updates(offset=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {"timeout": 10, "offset": offset}
    try:
        response = requests.get(url, params=params)
        return response.json()
    except:
        return {}

def send_telegram_message(chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": str(text)}
        requests.post(url, json=payload)
    except: pass

def answer_callback(callback_query_id, text):
    """Responde ao clique do botão para parar o loading visual"""
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery", 
                      json={"callback_query_id": callback_query_id, "text": text})
    except: pass

def edit_message_text(chat_id, message_id, text):
    """Edita a mensagem original para mostrar que foi processado"""
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText", 
                      json={
                          "chat_id": chat_id, 
                          "message_id": message_id, 
                          "text": text, 
                          "parse_mode": "Markdown",
                          "disable_web_page_preview": True
                      })
    except: pass

# --- INTELIGÊNCIA (Idêntica ao Original) ---

def update_history(chat_id, role, message):
    if chat_id not in user_histories:
        user_histories[chat_id] = []
    user_histories[chat_id].append(f"{role}: {message}")
    if len(user_histories[chat_id]) > HISTORY_LIMIT:
        user_histories[chat_id] = user_histories[chat_id][-HISTORY_LIMIT:]

def classify_intent_with_history(chat_id, current_text):
    try:
        model = genai.GenerativeModel(MODELO_PREFERIDO)
    except:
        model = genai.GenerativeModel('gemini-1.5-flash')

    history_block = "\n".join(user_histories.get(chat_id, []))
    
    prompt = f"""
    Você é um gerente de vendas experiente. Analise o histórico de conversa e a mensagem atual.
    
    HISTÓRICO RECENTE:
    {history_block}
    
    MENSAGEM ATUAL:
    User: {current_text}
    
    SUA MISSÃO:
    1. Entenda o contexto. Se o usuário disse "Campinas" agora, e antes disse "Escolas", o pedido é "Escolas em Campinas".
    2. Classifique em SEARCH ou CHAT.
    
    SAÍDA ESPERADA (JSON PURO):
    
    CASO 1: O usuário quer buscar leads/empresas (SEARCH).
    {{ 
      "type": "SEARCH", 
      "consolidated_query": "Escreva aqui o termo de busca COMPLETO e MELHORADO (ex: Startups de tecnologia em Piracicaba SP)" 
    }}
    
    CASO 2: Conversa fiada, dúvidas, 'oi', ou falta de informações claras (CHAT).
    {{ 
      "type": "CHAT", 
      "response": "Sua resposta simpática e curta perguntando mais detalhes." 
    }}
    """
    
    try:
        response = model.generate_content(prompt)
        text = response.text.replace('```json', '').replace('```', '').strip()
        data = json.loads(text)
        return data
    except Exception as e:
        print(f"⚠️ Erro IA: {e}")
        return {"type": "CHAT", "response": "Tive um erro de pensamento. Pode repetir o nicho?"}

# --- NOVO: GERENCIADOR DE CLIQUES (Callback) ---

def handle_callback_query(callback):
    """Processa o clique nos botões"""
    c_id = callback['id']
    chat_id = callback['message']['chat']['id']
    msg_id = callback['message']['message_id']
    data = callback['data'] # Ex: ENRICH:dominio.com
    original_text = callback['message']['text']
    
    if str(chat_id) not in ALLOWED_USERS:
        answer_callback(c_id, "⛔ Acesso Negado.")
        return

    print(f"🖱️ Clique: {data}")

    try:
        action, domain = data.split(":", 1)
        doc_ref = db.collection("leads_b2b").document(domain)

        # 1. DESCARTAR (Resolve Localmente)
        if action == "DISCARD":
            doc_ref.update({"status": "DISCARDED_BY_USER"})
            answer_callback(c_id, "🗑 Descartado.")
            # Atualiza texto visualmente
            new_text = original_text + "\n\n❌ *DESCARTADO*"
            edit_message_text(chat_id, msg_id, new_text)

        # 2. ENRIQUECER (Manda para Agent 3)
        elif action == "ENRICH":
            answer_callback(c_id, "🚀 Enviando para Agente 3...")
            
            # Payload específico para o Agent 3 (Messenger Mode)
            payload = {
                "command": "FETCH_PEOPLE",  # Flag para o Agente 3 saber que é ordem de busca
                "domain": domain,
                "chat_id": chat_id,
                "message_id": msg_id,       # Para o Agente 3 editar a mensagem depois
                "original_text_context": original_text
            }
            
            # Publica no Tópico do Agente 3 (topic-enricher)
            publisher.publish(topic_path_3, json.dumps(payload).encode("utf-8"))
            
            # Feedback Visual
            new_text = original_text + "\n\n⏳ *Solicitando enriquecimento...*"
            edit_message_text(chat_id, msg_id, new_text)

    except Exception as e:
        print(f"⚠️ Erro Callback: {e}")
        answer_callback(c_id, "Erro ao processar.")

# --- LOOP PRINCIPAL ---

def main():
    global last_update_id
    print(f"\n🤖 Bot Agente 0 RODANDO! (Search Original + Callbacks)")
    
    while True:
        updates = get_telegram_updates(last_update_id + 1)
        
        for update in updates.get("result", []):
            last_update_id = update["update_id"]
            
            # --- CASO A: Clique no Botão (NOVO) ---
            if "callback_query" in update:
                handle_callback_query(update["callback_query"])
                continue
            
            # --- CASO B: Mensagem de Texto (Lógica Original Mantida) ---
            if "message" in update and "text" in update["message"]:
                chat_id = update["message"]["chat"]["id"]
                text = update["message"]["text"]
                
                if str(chat_id) not in ALLOWED_USERS:
                    print(f"⛔ Acesso Negado: {chat_id}")
                    send_telegram_message(chat_id, "⛔ Acesso não autorizado.")
                    continue 

                print(f"\n📨 Mensagem de {chat_id}: {text}")
                
                decision = classify_intent_with_history(chat_id, text)
                tipo = decision.get('type', 'CHAT')
                update_history(chat_id, "User", text)

                if tipo == 'SEARCH':
                    query_consolidada = decision.get('consolidated_query', text)
                    print(f"🤔 Decisão: SEARCH -> '{query_consolidada}'")
                    send_telegram_message(chat_id, f"🔍 Entendido! Preparando busca para: {query_consolidada}")
                    
                    # Monta o Prompt com o Template Original
                    final_prompt_content = TEMPLATE_BUSCA.format(pedido=query_consolidada)
                    
                    # Payload Original para o Agente 1
                    payload = {
                        "command": final_prompt_content,
                        "chat_id": chat_id,
                        "original_term": query_consolidada
                    }
                    # Publica no Tópico do Agente 1 (topic-discovery-input)
                    publisher.publish(topic_path_1, json.dumps(payload).encode("utf-8"))
                    print("🚀 Enviado Template para Agente 1!")
                
                else:
                    resposta = decision.get('response')
                    print(f"🤔 Decisão: CHAT -> '{resposta}'")
                    send_telegram_message(chat_id, resposta)
                    update_history(chat_id, "Bot", resposta)

        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bot parado.")
