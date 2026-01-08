import os
import json
import warnings
import requests
from Wappalyzer import Wappalyzer, WebPage
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Ignora avisos chatos
warnings.filterwarnings("ignore")

load_dotenv()
print("\n🛠️ --- AGENTE 2: TECH DETECTIVE (HÍBRIDO: Wappalyzer + Custom) ---")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_OUTPUT = "topic-enricher" 
SUBSCRIPTION_INPUT = "sub-tech-checker"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_OUTPUT)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)

print("📚 Carregando cérebro do Wappalyzer...")
wappalyzer = Wappalyzer.latest()
print("✅ Pronto!")

def check_custom_signals(html_content):
    """
    Busca manual (Regex/String) para ferramentas que o Wappalyzer costuma ignorar
    se estiverem dentro do GTM ou ofuscadas.
    """
    html = html_content.lower()
    found = []
    
    # Dicionário de 'Faro Comercial'
    signals = {
        "rdstation": "RD Station",
        "hubspot": "HubSpot",
        "vtex": "VTEX",
        "shopify": "Shopify",
        "hotmart": "Hotmart",
        "eduzz": "Eduzz",
        "activecampaign": "ActiveCampaign",
        "mailchimp": "Mailchimp",
        "pipedrive": "Pipedrive",
        "salesforce": "Salesforce",
        "whatsapp": "Botão WhatsApp",
        "facebook.com/tr": "Pixel Facebook",
        "googletagmanager": "Google Tag Manager"
    }
    
    for key, name in signals.items():
        if key in html:
            found.append(name)
            
    return found

def analyze_domain(domain):
    if not domain.startswith("http"): url = f"http://{domain}"
    else: url = domain
    
    print(f"🕵️ Analisando: {domain}...")
    try:
        # 1. Baixa o site uma única vez
        webpage = WebPage.new_from_url(url, timeout=15)
        
        # 2. Análise Técnica (Wappalyzer)
        techs_wappalyzer = wappalyzer.analyze_with_versions_and_categories(webpage)
        list_wappalyzer = list(techs_wappalyzer.keys())
        
        # 3. Análise Comercial (Nosso Script)
        list_custom = check_custom_signals(webpage.html)
        
        # 4. Fusão (Remove duplicadas)
        final_techs = list(set(list_wappalyzer + list_custom))
        
        return final_techs

    except Exception as e:
        print(f"⚠️ Erro ao analisar {domain}: {str(e)[:100]}")
        return []

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        domain = data.get("domain")
        chat_id = data.get("chat_id")
        origin_query = data.get("origin_query")
        
        techs = analyze_domain(domain)
        
        if techs:
            # --- MUDANÇA AQUI: MOSTRA A LISTA COMPLETA ---
            print(f"✅ {domain}: {techs}") 
            
            # Destaque especial para RD Station
            if "RD Station" in techs: print("   🎯 ACHOU RD STATION!")

            payload = {
                "domain": domain,
                "techs": techs,
                "chat_id": chat_id,
                "origin_query": origin_query
            }
            publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
        else:
            print(f"❌ {domain}: Falha ou site offline.")
            
        message.ack()
    except Exception as e:
        print(f"🔥 Erro Worker: {e}")
        message.nack()

if __name__ == "__main__":
    print(f"🛠️ Agente 2 Híbrido ouvindo em: {SUBSCRIPTION_INPUT}...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback).result()
        except KeyboardInterrupt: pass
