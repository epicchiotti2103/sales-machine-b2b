import os
import json
import re
import requests
from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()
print("\n💎 --- AGENTE 3: ATAQUE TOTAL (Apollo > Scraping > Lusha) ---")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TOPIC_OUTPUT = "topic-closer-hubspot"
SUBSCRIPTION_INPUT = "sub-enricher-worker"

APOLLO_KEY = os.getenv("APOLLO_API_KEY")
LUSHA_KEY = os.getenv("LUSHA_API_KEY")

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_OUTPUT)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)

def notify_telegram(chat_id, text):
    if not chat_id: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                      json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    except: pass

# --- 1. APOLLO (Busca Nomes) ---
def find_people_apollo(domain):
    if not APOLLO_KEY: return "❌ Sem Chave", []
    
    url = "https://api.apollo.io/v1/mixed_people/search"
    # Sem filtro de cargo para maximizar chances
    payload = {
        "q_organization_domains": [domain],
        "page": 1, 
        "per_page": 2
    }
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_KEY}
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get('people', [])
            found = []
            for p in data:
                if p.get('first_name') and p.get('last_name'):
                    found.append({
                        "first": p.get('first_name'),
                        "last": p.get('last_name'),
                        "title": p.get('title') or "Cargo n/d"
                    })
            
            if found: return f"✅ Achou {len(found)} nomes", found
            else: return "⚠️ 0 Nomes", []
        else:
            return f"⚠️ Erro {resp.status_code}", []
    except Exception as e:
        return f"🔥 Erro Apollo", []

# --- 2. LUSHA (Valida Nomes OU E-mails) ---
def enrich_lusha_v2(domain, first=None, last=None, email=None):
    if not LUSHA_KEY: return "❌ Sem Chave"
    
    url = "https://api.lusha.com/v2/person"
    headers = {"Content-Type": "application/json", "api_key": LUSHA_KEY}
    
    contact_obj = {"contactId": "1"}
    
    # Lógica inteligente: Usa o que tiver disponível
    if email:
        contact_obj["email"] = email
        log_msg = f"validando email '{email}'"
    elif first and last:
        contact_obj["fullName"] = f"{first} {last}"
        contact_obj["companies"] = [{"domain": domain, "isCurrent": True}]
        log_msg = f"buscando '{first} {last}'"
    else:
        return "Dados insuficientes"

    # Payload Oficial Lusha V2 (POST)
    payload = {
        "contacts": [contact_obj],
        "metadata": {"revealEmails": True, "revealPhones": True}
    }

    try:
        # print(f"   👉 Lusha: {log_msg}...") # Debug opcional
        resp = requests.post(url, json=payload, headers=headers, timeout=8)
        
        if resp.status_code == 200:
            data = resp.json()
            c_data = data.get('contacts', {}).get('1', {})
            
            # Recupera dados enriquecidos
            found_email = c_data.get('email')
            found_phone = c_data.get('phone')
            
            # Formata retorno
            ret = []
            if found_email: ret.append(f"📧 {found_email}")
            if found_phone: ret.append(f"📱 {found_phone}")
            
            if ret: return " | ".join(ret)
            else: return "Lusha não achou extras"
            
        elif resp.status_code == 401: return "❌ Erro 401 (Chave)"
        elif resp.status_code == 429: return "❌ Erro 429 (Limite)"
        else: return f"⚠️ Status {resp.status_code}"
            
    except Exception as e:
        return f"Erro Req"

# --- 3. SCRAPING (Busca E-mails no Site) ---
def run_scraping(domain):
    if not domain.startswith("http"): url = f"http://{domain}"
    else: url = domain
    try:
        resp = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        html = resp.text
        
        # Regex para emails
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        found = set(re.findall(email_pattern, html))
        # Filtra lixo
        clean = [e for e in found if not e.endswith(('.png', '.jpg', '.js', 'wix.com', 'sentry.io'))]
        
        contacts = list(clean)[:3]
        
        socials = []
        if "instagram.com" in html: socials.append("Insta")
        if "wa.me" in html: socials.append("Whats")
        
        return contacts, socials
    except: return [], []

# --- 4. ORQUESTRAÇÃO ---
def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        domain = data.get("domain")
        chat_id = data.get("chat_id")
        techs = data.get("techs", [])

        print(f"\n🔵 === PROCESSANDO: {domain} ===")
        
        final_contacts = []
        sources = []
        
        # PASSO A: APOLLO (Tenta Nomes)
        status_apollo, people_list = find_people_apollo(domain)
        print(f"   [APOLLO]   {status_apollo}")
        
        if people_list:
            sources.append("Apollo")
            # Se achou nomes, manda pro Lusha
            for p in people_list:
                res = enrich_lusha_v2(domain, first=p['first'], last=p['last'])
                if "❌" not in res and "⚠️" not in res:
                    final_contacts.append(f"👤 {p['first']} {p['last']} ({p['title']})\n   └ {res} _[Lusha]_")
        
        # PASSO B: SCRAPING (Tenta E-mails)
        site_emails, socials = run_scraping(domain)
        
        if site_emails:
            print(f"   [SCRAP]    ✅ Achou {len(site_emails)} emails")
            sources.append("Site")
            
            # PASSO C: LUSHA REVERSO (Tenta enriquecer o email do site)
            # Se Apollo falhou em nomes, usamos os emails do site para tentar achar a pessoa no Lusha
            if not people_list:
                for email in site_emails:
                    res = enrich_lusha_v2(domain, email=email)
                    if "❌" not in res and "⚠️" not in res and "não achou" not in res:
                         final_contacts.append(f"📧 {email}\n   └ Detalhes Lusha: {res}")
                         if "LushaReverso" not in sources: sources.append("LushaReverso")
                    else:
                         final_contacts.append(f"📧 {email} _[Site]_")
        else:
            print(f"   [SCRAP]    ⚠️ Nada no site")

        print("   -------------------------------")

        # SCORE
        score = 20
        if people_list: score += 30
        if any("Lusha" in c for c in final_contacts): score += 40
        if "RD Station" in str(techs): score += 10
        score = min(score, 100)

        # TELEGRAM MSG
        msg = f"🎯 *LEAD PROCESSADO*\n"
        msg += f"🏢 *{domain}*\n"
        msg += f"📊 Score: {score}/100\n"
        msg += f"🛠 Tech: {', '.join(techs[:3])}\n"
        if socials: msg += f"🔗 {' | '.join(socials)}\n"
        msg += "----------------\n"
        
        if final_contacts:
            msg += "\n".join(final_contacts[:5])
        else:
            msg += "❌ Nenhum contato encontrado."
        
        msg += f"\n\n⚙️ Fontes: {', '.join(sources) if sources else 'Nenhuma'}"
        
        notify_telegram(chat_id, msg)
        publisher.publish(topic_path, json.dumps({"domain": domain}).encode("utf-8"))
        message.ack()

    except Exception as e:
        print(f"🔥 Erro Geral: {e}")
        message.nack()

if __name__ == "__main__":
    print(f"💎 Agente 3 (Ataque Total) ouvindo...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback).result()
        except KeyboardInterrupt: pass
