import os
import time
import json
import requests
import google.generativeai as genai
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# --- Configuração Inicial ---
load_dotenv()
print("\n🔥 --- AGENTE 0: O PORTEIRO (V3.2 - Memória + Contexto Rico) ---")

# 1. Carrega Variáveis
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_AGENT_1 = "topic-discovery-input"

# Carrega lista de usuários permitidos
ALLOWED_USERS_RAW = os.getenv("ALLOWED_USERS", "")
ALLOWED_USERS = [id.strip() for id in ALLOWED_USERS_RAW.split(",") if id.strip()]

# Modelo mantido
MODELO_PREFERIDO = "gemini-2.0-flash-lite-preview-02-05" 

# --- MEMÓRIA VOLÁTIL (Salva em RAM enquanto o script roda) ---
# Estrutura: { chat_id: ["User: msg", "Bot: msg", ...] }
user_histories = {}
HISTORY_LIMIT = 6  # Mantém últimas 3 conversas (3 perguntas + 3 respostas)

# --- SEU TEMPLATE DE BUSCA (A ser preenchido) ---
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
else:
    print(f"🔒 Segurança Ativa: {len(ALLOWED_USERS)} usuários autorizados.")

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
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": str(text)}
        requests.post(url, json=payload)
    except: pass

def update_history(chat_id, role, message):
    """Gerencia a memória de curto prazo do usuário."""
    if chat_id not in user_histories:
        user_histories[chat_id] = []
    
    # Adiciona nova mensagem
    user_histories[chat_id].append(f"{role}: {message}")
    
    # Mantém apenas as últimas X mensagens (Janela Deslizante)
    if len(user_histories[chat_id]) > HISTORY_LIMIT:
        user_histories[chat_id] = user_histories[chat_id][-HISTORY_LIMIT:]

def classify_intent_with_history(chat_id, current_text):
    """Analisa intenção considerando o histórico da conversa."""
    try:
        model = genai.GenerativeModel(MODELO_PREFERIDO)
    except:
        model = genai.GenerativeModel('gemini-1.5-flash')

    # Recupera histórico formatado
    history_block = "\n".join(user_histories.get(chat_id, []))
    
    print(f"🧠 Analisando histórico de {chat_id}...")
    
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

def main():
    global last_update_id
    print(f"\n🤖 Bot Agente 0 RODANDO! (Com Memória)")
    
    while True:
        updates = get_telegram_updates(last_update_id + 1)
        
        for update in updates.get("result", []):
            last_update_id = update["update_id"]
            
            if "message" in update and "text" in update["message"]:
                chat_id = update["message"]["chat"]["id"]
                text = update["message"]["text"]
                
                # --- VERIFICAÇÃO DE SEGURANÇA ---
                if str(chat_id) not in ALLOWED_USERS:
                    print(f"⛔ Acesso Negado: {chat_id}")
                    send_telegram_message(chat_id, "⛔ Acesso não autorizado.")
                    continue 

                print(f"\n📨 Mensagem de {chat_id}: {text}")
                
                # 1. Decide Intenção com Memória
                decision = classify_intent_with_history(chat_id, text)
                tipo = decision.get('type', 'CHAT')
                
                # 2. Atualiza Histórico com o que o user disse
                update_history(chat_id, "User", text)

                if tipo == 'SEARCH':
                    # Pega a query "inteligente" que o Gemini consolidou
                    query_consolidada = decision.get('consolidated_query', text)
                    
                    print(f"🤔 Decisão: SEARCH -> '{query_consolidada}'")
                    send_telegram_message(chat_id, f"🔍 Entendido! Preparando busca para: {query_consolidada}")
                    
                    # 3. Monta o PROMPT GIGANTE (Template)
                    # Atenção: Usamos .format() ou f-string com cuidado por causa das chaves do JSON
                    final_prompt_content = TEMPLATE_BUSCA.format(pedido=query_consolidada)
                    
                    # 4. Manda para o Agente 1 (Perplexity)
                    payload = {
                        "command": final_prompt_content, # O Agente 1 vai receber o Prompt Inteiro aqui
                        "chat_id": chat_id,
                        "original_term": query_consolidada # Útil para logs futuros
                    }
                    publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
                    print("🚀 Enviado Template para Pub/Sub!")
                    
                    # Limpa histórico após uma busca bem sucedida para evitar confusão no próximo tema?
                    # Opcional. Por enquanto mantemos para contexto contínuo.
                
                else:
                    resposta = decision.get('response')
                    print(f"🤔 Decisão: CHAT -> '{resposta}'")
                    send_telegram_message(chat_id, resposta)
                    
                    # Atualiza histórico com a resposta do bot
                    update_history(chat_id, "Bot", resposta)

        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bot parado.")
