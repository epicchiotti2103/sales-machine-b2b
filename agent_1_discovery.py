
import os
import json
import requests
import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# --- IMPORTAÇÃO DO BANCO ---
try:
    import database
    print("✅ Módulo database carregado.")
except ImportError:
    print("⚠️ ERRO: database.py não encontrado.")
    class database:
        @staticmethod
        def check_lead_exists(domain): return False
        @staticmethod
        def save_new_lead(domain, query): pass 

# --- Configuração ---
load_dotenv()
print("\n🕵️ --- AGENTE 1: DISCOVERY (V4.3 - Filtro Anti-Hub) ---")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
SUBSCRIPTION_NAME = "sub-telegram-input" 
NEXT_TOPIC_NAME = "topic-tech-filter"    
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") 

MAX_RETRIES = 1  

if not PERPLEXITY_API_KEY:
    print("❌ ERRO: PERPLEXITY_API_KEY não configurada!")
    exit()

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, NEXT_TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

def notify_telegram(chat_id, text):
    if not chat_id: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    except: pass

def clean_url(url):
    if not url: return None
    clean = url.lower().replace("https://", "").replace("http://", "").replace("www.", "")
    if "/" in clean: clean = clean.split("/")[0]
    clean = clean.strip()
    if " " in clean or "." not in clean or len(clean) < 4: return None
    return clean

def is_blacklisted(domain):
    blacklist = [
        "instagram.com", "facebook.com", "linkedin.com", "youtube.com", 
        "twitter.com", "google.com", "linktr.ee", "whatsapp.com",
        "t.me", "goo.gl", "bit.ly", "reclameaqui.com.br", "glassdoor.com",
        ".org" # <-- BLOQUEIO DE ONGs/ASSOCIAÇÕES
    ]
    return any(b in domain for b in blacklist)

def search_perplexity_v3(prompt_completo):
    print(f"🔎 Consultando Perplexity...")
    
    # --- AQUI ESTÁ A MUDANÇA NO PROMPT ---
    system_instruction = """
    Você é um assistente B2B focado em vendas. 
    Responda APENAS o JSON solicitado.
    
    REGRAS DE EXCLUSÃO (CRÍTICO):
    1. ESTRITAMENTE PROIBIDO retornar: Hubs de inovação, Aceleradoras, Associações, Sindicatos, Ligas Acadêmicas, Universidades Públicas ou Portais de Notícias.
    2. Quero apenas as EMPRESAS FINAIS (CNPJs) que vendem produtos/serviços.
    3. Se a busca for por startups, ignore quem "apoia" startups e liste as startups em si.
    """
    
    url = "https://api.perplexity.ai/chat/completions"
    payload = {
        "model": "sonar", 
        "messages": [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt_completo}
        ],
        "temperature": 0.2 # Baixei para 0.2 para ele ser mais obediente
    }
    headers = {"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        if response.status_code != 200:
            print(f"⚠️ API Status: {response.status_code}")
            return []

        content = response.json()['choices'][0]['message']['content']
        start = content.find('{')
        end = content.rfind('}') + 1
        if start == -1 or end == 0: return []
        
        data = json.loads(content[start:end])
        return data.get('companies', [])
    except Exception as e:
        print(f"❌ Erro API: {e}")
        return []

def callback(message):
    try:
        print(f"\n📨 Novo Pedido...")
        data = json.loads(message.data.decode("utf-8"))
        base_prompt = data.get("command")
        chat_id = data.get("chat_id")
        query = data.get("original_term", "Busca")
        
        attempt = 0
        leads_enviados_total = 0
        leads_enviados_nomes = [] 
        exclude_names = [] 
        
        while attempt <= MAX_RETRIES:
            print(f"   🔄 Tentativa {attempt + 1} de {MAX_RETRIES + 1}...")
            
            current_prompt = base_prompt
            if attempt > 0 and exclude_names:
                exclusions_str = ", ".join(exclude_names[:15]) 
                print(f"   🚫 Excluindo da busca: {exclusions_str[:50]}...")
                current_prompt += f"\n\nIMPORTANTE: Busque OUTRAS empresas. NÃO inclua estas: {exclusions_str}."

            companies = search_perplexity_v3(current_prompt)
            
            if not companies:
                print("   ⚠️ Perplexity não retornou nada nesta tentativa.")
                break 

            print(f"   ✅ Retornou {len(companies)} candidatos.")
            
            new_in_this_batch = 0
            
            for comp in companies:
                domain = clean_url(comp.get('website'))
                company_name = comp.get("name", domain)
                
                # O filtro .org vai pegar aqui
                if not domain or is_blacklisted(domain):
                    print(f"      🗑️ Ignorado (Blacklist/Org): {domain}")
                    continue
                
                if database.check_lead_exists(domain):
                    print(f"      ⏩ {domain}: Já existe.")
                    if company_name not in exclude_names:
                        exclude_names.append(company_name)
                    continue
                
                # É NOVO!
                database.save_new_lead(domain, query)
                
                payload = {
                    "domain": domain,
                    "origin_query": query,
                    "chat_id": chat_id,
                    "context_data": {
                        "name": company_name,
                        "sector": comp.get("sector"),
                        "size": comp.get("size"),
                        "fit_explanation": comp.get("fit_explanation")
                    }
                }
                publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
                print(f"      🚀 {domain}: Enviado!")
                leads_enviados_total += 1
                leads_enviados_nomes.append(f"• {company_name} ({domain})")
                new_in_this_batch += 1
            
            if new_in_this_batch < 2 and attempt < MAX_RETRIES:
                print("   🤔 Poucos leads novos. Insistindo...")
                attempt += 1
                time.sleep(2) 
                continue
            else:
                break

        # --- RELATÓRIO FINAL ---
        msg = f"📊 *Relatório Final: {query}*\n"
        
        if leads_enviados_total > 0:
            msg += f"✅ Enviados para esteira: {leads_enviados_total}\n"
            msg += "\n".join(leads_enviados_nomes)
        else:
            msg += f"⚠️ Nenhum lead NOVO encontrado após {attempt+1} tentativas.\n"
            msg += "Tente refinar a busca."

        notify_telegram(chat_id, msg)
        message.ack()
        
    except Exception as e:
        print(f"🔥 Erro: {e}")
        message.nack()

if __name__ == "__main__":
    print(f"🎧 Agente 1 (V4.3 - Anti-Hub) ouvindo...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback).result()
        except KeyboardInterrupt: pass
