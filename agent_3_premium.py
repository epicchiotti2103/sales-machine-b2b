
import os
import json
import requests
import threading
from google.cloud import pubsub_v1
from dotenv import load_dotenv

try:
    import database
    print("✅ [Agente 3] Módulo database carregado.")
except ImportError:
    print("⚠️ [Agente 3] ERRO: database.py não encontrado.")
    class database:
        @staticmethod
        def update_enrichment(domain, data): pass

load_dotenv()
print("\n💎 --- AGENTE 3: ENRICHER (V3.5 - Smart Receiver) ---")

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
                      json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=3)
    except: pass

def find_people_apollo(domain):
    if not APOLLO_KEY: return "❌ Sem Chave", []
    url = "https://api.apollo.io/v1/mixed_people/search"
    payload = {"q_organization_domains": [domain], "page": 1, "per_page": 3}
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_KEY}
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get('people', [])
            found = []
            for p in data:
                if p.get('first_name') and p.get('last_name'):
                    found.append({"first": p.get('first_name'), "last": p.get('last_name'), "title": p.get('title') or "Cargo n/d"})
            if found: return f"✅ Achou {len(found)} nomes", found
            else: return "⚠️ 0 Nomes", []
        else: return f"⚠️ Erro {resp.status_code}", []
    except: return f"🔥 Erro Apollo", []

def enrich_lusha_v2(domain, first=None, last=None, email=None):
    if not LUSHA_KEY: return "❌ Sem Chave"
    url = "https://api.lusha.com/v2/person"
    headers = {"Content-Type": "application/json", "api_key": LUSHA_KEY}
    
    contact_obj = {"contactId": "1"}
    
    # Lógica de input para Lusha
    if email: 
        contact_obj["email"] = email
    elif first and last:
        contact_obj["fullName"] = f"{first} {last}"
        contact_obj["companies"] = [{"domain": domain, "isCurrent": True}]
    else: 
        return "Dados insuficientes"
        
    payload = {"contacts": [contact_obj], "metadata": {"revealEmails": True, "revealPhones": True}}
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            c_data = data.get('contacts', {}).get('1', {})
            ret = []
            if c_data.get('email'): ret.append(f"📧 {c_data.get('email')}")
            if c_data.get('phone'): ret.append(f"📱 {c_data.get('phone')}")
            return " | ".join(ret) if ret else "Lusha não achou extras"
        elif resp.status_code == 401: return "❌ Erro 401 (Chave)"
        elif resp.status_code == 429: return "❌ Erro 429 (Limite)"
        else: return f"⚠️ Status {resp.status_code}"
    except: return f"Erro Req"

def callback(message):
    timer_loading = None
    try:
        data = json.loads(message.data.decode("utf-8"))
        domain = data.get("domain")
        chat_id = data.get("chat_id")
        
        if chat_id:
            timer_loading = threading.Timer(10.0, notify_telegram, args=[chat_id, "⏳ _Analisando a fundo... (Isso pode levar alguns segundos)_"])
            timer_loading.start()

        # Dados do Agente 2
        techs = data.get("techs", [])
        tech_summary = data.get("tech_summary", {})
        tech_score = data.get("tech_score", 0) 
        hosting = data.get("hosting", "N/D")
        site_emails = data.get("site_emails", []) # <-- NOVO: Recebe direto
        site_socials = data.get("site_socials", []) # <-- NOVO: Recebe direto
        
        print(f"\n🔵 === PROCESSANDO: {domain} ===")
        print(f"   [INPUT]    Recebido do Agente 2: {len(site_emails)} emails, {len(site_socials)} socials")
        
        final_contacts = []
        sources = []
        
        # A. APOLLO (Tenta nomes específicos primeiro)
        status_apollo, people_list = find_people_apollo(domain)
        print(f"   [APOLLO]   {status_apollo}")
        
        if people_list:
            sources.append("Apollo")
            for p in people_list:
                # Tenta enriquecer o contato achado no Apollo
                res = enrich_lusha_v2(domain, first=p['first'], last=p['last'])
                if "❌" not in res and "⚠️" not in res:
                    final_contacts.append(f"👤 {p['first']} {p['last']} ({p['title']})\n   └ {res} _[Lusha]_")
        
        # B. REAPROVEITA DADOS DO SITE (Se Apollo falhou ou para complementar)
        if site_emails:
            if "Site" not in sources: sources.append("Site (Agente 2)")
            
            # Se não achou NINGUÉM no Apollo, tenta enriquecer os emails genéricos do site
            if not people_list:
                for email in site_emails[:3]: # Limita a 3 para não estourar API
                    print(f"   [LUSHA R]  Tentando reverso em: {email}")
                    res = enrich_lusha_v2(domain, email=email)
                    if "❌" not in res and "⚠️" not in res and "não achou" not in res:
                         final_contacts.append(f"📧 {email}\n   └ Detalhes: {res}")
                         if "LushaReverso" not in sources: sources.append("LushaReverso")
                    else:
                         final_contacts.append(f"📧 {email} _[Site]_")
            else:
                # Se já achou gente no Apollo, só lista os emails do site sem gastar Lusha (Economia)
                for email in site_emails[:2]:
                    final_contacts.append(f"📧 {email} _[Site]_")

        # C. Cálculo de Score
        contact_score = 0
        if people_list: contact_score += 40
        elif site_emails: contact_score += 20
        if "LushaReverso" in sources: contact_score += 20
        
        final_score = int((contact_score * 0.6) + (tech_score * 0.4))
        if final_score > 100: final_score = 100

        # D. Montagem da Mensagem
        if tech_score > 0: tech_display = f"{tech_score}/100"
        else: tech_display = "-" 

        msg_tech = ""
        used_techs = set()
        
        if tech_summary:
            if tech_summary.get('marketing'):
                t_list = tech_summary['marketing']
                msg_tech += f"📢 *Mkt:* {', '.join(t_list)}\n"
                used_techs.update(t_list)
            if tech_summary.get('ecommerce'):
                t_list = tech_summary['ecommerce']
                msg_tech += f"🛒 *Ecom:* {', '.join(t_list)}\n"
                used_techs.update(t_list)
            if tech_summary.get('cms'):
                t_list = tech_summary['cms']
                msg_tech += f"📝 *CMS:* {', '.join(t_list)}\n"
                used_techs.update(t_list)
            if tech_summary.get('analytics'):
                t_list = tech_summary['analytics']
                msg_tech += f"📈 *Data:* {', '.join(t_list)}\n"
                used_techs.update(t_list)
            
            leftovers = [t for t in techs if t not in used_techs]
            if leftovers:
                msg_tech += f"⚙️ *Infra/Outros:* {', '.join(leftovers)}\n"
        elif techs:
            msg_tech = f"🛠 Stack: {', '.join(techs)}\n"
        else:
            msg_tech = "⚠️ Nenhuma tecnologia identificada.\n"

        msg = f"🎯 *LEAD ENRIQUECIDO*\n"
        msg += f"🏢 *{domain}*\n"
        msg += f"☁️ Host: {hosting}\n"
        msg += "----------------\n"
        msg += f"📊 *Tech Score:* {tech_display}\n" 
        msg += msg_tech
        msg += "----------------\n"
        
        if site_socials: 
            # Limpa a formatação para ficar bonito no Telegram
            clean_socials = [s.replace("Instagram: https://www.instagram.com/", "Insta: @").replace("Facebook: https://www.facebook.com/", "Face: /") for s in site_socials]
            msg += f"📱 Social: {' | '.join(clean_socials[:3])}\n"
        
        if final_contacts:
            msg += "\n".join(final_contacts[:5])
        else:
            msg += "❌ Nenhum contato direto encontrado."
        
        msg += f"\n\n⚙️ Fontes: {', '.join(sources) if sources else 'Nenhuma'}"
        
        if timer_loading: timer_loading.cancel()

        # Update Final
        database.update_enrichment(domain, {
            "final_score": final_score, 
            "contacts_found": len(final_contacts),
            "sources": sources
        })
        
        notify_telegram(chat_id, msg)
        
        payload_closer = {"domain": domain, "final_score": final_score}
        publisher.publish(topic_path, json.dumps(payload_closer).encode("utf-8"))
        
        message.ack()

    except Exception as e:
        if timer_loading: timer_loading.cancel()
        print(f"🔥 Erro Geral: {e}")
        message.nack()

if __name__ == "__main__":
    flow_control = pubsub_v1.types.FlowControl(max_messages=5)
    print(f"💎 Agente 3 (Enricher v3.5 - Smart Receiver) ouvindo...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control).result()
        except KeyboardInterrupt: pass
