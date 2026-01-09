import os
import json
import warnings
import requests
import re
from Wappalyzer import Wappalyzer, WebPage
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# --- IMPORTAÇÃO SEGURA DO BANCO ---
try:
    import database
    print("✅ [Agente 2] Módulo database carregado.")
except ImportError:
    print("⚠️ [Agente 2] ERRO: database.py não encontrado.")
    class database:
        @staticmethod
        def update_techs(domain, techs): pass

warnings.filterwarnings("ignore")
load_dotenv()

print("\n🛠️ --- AGENTE 2: TECH ANALYST (V3.1 - Full Payload) ---")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_OUTPUT = "topic-enricher" 
SUBSCRIPTION_INPUT = "sub-tech-checker"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_OUTPUT)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)

print("📚 Carregando cérebro do Wappalyzer...")
wappalyzer = Wappalyzer.latest()
print("✅ Pronto! Aguardando leads...")

# --- DICIONÁRIO DE INTELIGÊNCIA ---
CUSTOM_SIGNALS = {
    "RD Station": {"patterns": [r"d335luupugsy2\.cloudfront\.net", r"rdstation\.com\.br", r"rd_station"], "cat": "Marketing", "score": 10},
    "HubSpot": {"patterns": [r"js\.hs-scripts\.com", r"js\.hs-analytics\.net", r"hubspot\.com"], "cat": "CRM/Marketing", "score": 20},
    "Salesforce": {"patterns": [r"force\.com", r"salesforce\.com"], "cat": "CRM Enterprise", "score": 30},
    "VTEX": {"patterns": [r"vteximg\.com\.br", r"vtex\.com", r"io\.vtex\.com"], "cat": "Ecommerce", "score": 25},
    "Shopify": {"patterns": [r"cdn\.shopify\.com", r"shopify\.com"], "cat": "Ecommerce", "score": 15},
    "Nuvemshop": {"patterns": [r"nuvemshop\.com\.br", r"lojanuvem\.com"], "cat": "Ecommerce", "score": 10},
    "Hotmart": {"patterns": [r"hotmart\.com", r"laucher\.hotmart"], "cat": "Infoproduto", "score": 10},
    "WordPress": {"patterns": [r"wp-content", r"wp-includes"], "cat": "CMS", "score": 5},
    "WooCommerce": {"patterns": [r"woocommerce"], "cat": "Ecommerce", "score": 5},
    "Wix": {"patterns": [r"wix\.com", r"wix-waypoint"], "cat": "CMS Básico", "score": 1},
    "Google Analytics 4": {"patterns": [r"googletagmanager\.com/gtag/js", r"g-([a-z0-9]+)"], "cat": "Analytics", "score": 5},
    "Google Tag Manager": {"patterns": [r"googletagmanager\.com/gtm\.js"], "cat": "Analytics", "score": 5},
    "Meta Pixel": {"patterns": [r"connect\.facebook\.net/en_us/fbevents\.js", r"fbq\('init'"], "cat": "Ads", "score": 5},
    "JivoChat": {"patterns": [r"code\.jivosite\.com"], "cat": "Chat", "score": 5},
    "Zendesk": {"patterns": [r"zdassets\.com", r"zendesk\.com"], "cat": "Suporte", "score": 15}
}

def analyze_advanced_signals(html_content):
    if not html_content: return []
    html = html_content[:500000].lower()
    found_techs = []
    found_names = set()

    for name, info in CUSTOM_SIGNALS.items():
        for pattern in info["patterns"]:
            if re.search(pattern, html):
                if name not in found_names:
                    found_techs.append({
                        "name": name,
                        "category": info["cat"],
                        "score": info["score"],
                        "source": "Custom Regex"
                    })
                    found_names.add(name)
                break 
    return found_techs

def get_hosting_provider(wappa_results, html):
    html_safe = html.lower() if html else ""
    if "amazonaws" in html_safe or "aws" in html_safe: return "AWS (Amazon)"
    if "googleapis" in html_safe or "google cloud" in html_safe: return "Google Cloud"
    if "azure" in html_safe: return "Microsoft Azure"
    if "cloudflare" in html_safe: return "Cloudflare"
    if "locaweb" in html_safe: return "Locaweb"
    if "hostgator" in html_safe: return "Hostgator"
    return "Não identificado/Outro"

def process_wappalyzer_result(wappa_raw):
    processed = []
    cat_score_map = {
        "Ecommerce": 10, "CRM": 20, "Marketing Automation": 15, 
        "Analytics": 5, "Advertising": 5, "CMS": 5, "Web Servers": 1
    }
    for name, data in wappa_raw.items():
        cats = data.get('categories', [])
        main_cat = cats[0] if cats else "Outros"
        score = 1
        for c in cats:
            if c in cat_score_map: score = max(score, cat_score_map[c])
        processed.append({
            "name": name,
            "category": main_cat,
            "score": score,
            "source": "Wappalyzer"
        })
    return processed

def analyze_domain(domain):
    if not domain.startswith("http"): url = f"http://{domain}"
    else: url = domain
    
    print(f"   🕵️ Acessando: {domain}...") 
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36"}

    try:
        resp = requests.get(url, timeout=4, headers=headers)
        
        webpage = WebPage(resp.url, resp.text, resp.headers)
        wappa_raw = wappalyzer.analyze_with_versions_and_categories(webpage)
        techs_wappa = process_wappalyzer_result(wappa_raw)
        techs_custom = analyze_advanced_signals(resp.text)
        hosting = get_hosting_provider(wappa_raw, resp.text)
        
        final_dict = {}
        for t in techs_wappa: final_dict[t['name']] = t
        for t in techs_custom: final_dict[t['name']] = t
        
        tech_list = list(final_dict.values())
        total_score = sum([t['score'] for t in tech_list])
        
        summary = {
            "marketing": [t['name'] for t in tech_list if t['category'] in ["Marketing", "CRM", "Ads", "Marketing Automation"]],
            "ecommerce": [t['name'] for t in tech_list if t['category'] in ["Ecommerce"]],
            "cms": [t['name'] for t in tech_list if t['category'] in ["CMS", "CMS Básico"]],
            "analytics": [t['name'] for t in tech_list if t['category'] in ["Analytics"]]
        }

        return {
            "tech_list": tech_list,
            "tech_names": [t['name'] for t in tech_list],
            "summary": summary,
            "total_score": min(total_score, 100),
            "hosting": hosting
        }

    except requests.exceptions.Timeout:
        print(f"   ⌛ Timeout (4s): {domain}")
        return None
    except Exception as e:
        print(f"   ⚠️ Erro: {str(e)[:50]}")
        return None

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        domain = data.get("domain")
        chat_id = data.get("chat_id")
        origin_query = data.get("origin_query")
        
        print(f"\n📨 RECEBIDO: {domain}") 
        
        result = analyze_domain(domain)
        
        if result:
            score = result['total_score']
            host = result['hosting']
            techs = result['tech_names']
            # AQUI ESTAVA O ERRO: Precisamos pegar o summary para enviar
            summary = result['summary']
            
            print(f"   ✅ Score: {score}/100 | Host: {host}")
            
            database_payload = {
                "techs": techs,
                "tech_details": result,
                "tech_score": score,
                "tech_date": "NOW"
            }
            database.update_techs(domain, database_payload)

            # Publica com o 'tech_summary' incluso
            payload = {
                "domain": domain,
                "techs": techs,
                "tech_summary": summary, # ADICIONADO!
                "tech_score": score,
                "hosting": host,
                "chat_id": chat_id,
                "origin_query": origin_query
            }
            publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
        else:
            print(f"   💨 Falha/Offline. Ignorando.")

        message.ack()
        
    except Exception as e:
        print(f"🔥 Erro Worker: {e}")
        message.ack()

if __name__ == "__main__":
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    print(f"🛠️ Ouvindo filas...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control).result()
        except KeyboardInterrupt: pass
