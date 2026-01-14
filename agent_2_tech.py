
import os
import json
import warnings
import requests
import re
import random
import time
from Wappalyzer import Wappalyzer, WebPage
from google.cloud import pubsub_v1
from dotenv import load_dotenv

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

print("\n🛠️ --- AGENTE 2: TECH ANALYST (V3.4 - Hybrid Scraper) ---")

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
    "Zendesk": {"patterns": [r"zdassets\.com", r"zendesk\.com"], "cat": "Suporte", "score": 15},
    "Adobe Experience Cloud": {"patterns": [r"assets\.adobedtm\.com", r"adobe\.com"], "cat": "Enterprise Marketing", "score": 25},
    "Oracle Commerce": {"patterns": [r"oracle\.com", r"atg\.com"], "cat": "Enterprise Ecommerce", "score": 25}
}

def get_stealth_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1"
    }

def extract_contact_info(html_content):
    """Extrai emails e redes sociais do HTML bruto."""
    if not html_content:
        return [], []
    
    # 1. Emails (com filtro de arquivos de imagem/js)
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    found_emails = set(re.findall(email_pattern, html_content))
    
    # Filtra extensões indesejadas e domínios genéricos de exemplo
    clean_emails = []
    ignored_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.js', '.css', '.svg', '.webp')
    ignored_domains = ('sentry.io', 'wix.com', 'example.com', 'domain.com')
    
    for email in found_emails:
        email_lower = email.lower()
        if not email_lower.endswith(ignored_extensions) and not any(d in email_lower for d in ignored_domains):
            clean_emails.append(email)
            
    # 2. Redes Sociais (Tenta pegar o link completo)
    found_socials = []
    
    # Regex para pegar links completos
    patterns = {
        "Instagram": r'https?://(?:www\.)?instagram\.com/[a-zA-Z0-9_.-]+',
        "LinkedIn": r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9_.-]+',
        "Facebook": r'https?://(?:www\.)?facebook\.com/[a-zA-Z0-9_.-]+',
        "WhatsApp": r'https?://(?:api\.whatsapp\.com/send|wa\.me)/\d+'
    }
    
    for net, pat in patterns.items():
        matches = re.findall(pat, html_content)
        if matches:
            # Pega o primeiro match de cada rede para não poluir
            found_socials.append(f"{net}: {matches[0]}")
    
    # Se não achou link completo, mas tem menção forte (fallback simples)
    if not any("Instagram" in s for s in found_socials) and "instagram.com" in html_content:
        found_socials.append("Instagram (Link n/d)")
        
    return list(set(clean_emails))[:5], found_socials

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
    if "akamai" in html_safe: return "Akamai (Enterprise)"
    if "locaweb" in html_safe: return "Locaweb"
    if "hostgator" in html_safe: return "Hostgator"
    return "Não identificado/Outro"

def process_wappalyzer_result(wappa_raw):
    processed = []
    cat_score_map = {
        "Ecommerce": 10, "CRM": 20, "Marketing Automation": 15, 
        "Analytics": 5, "Advertising": 5, "CMS": 5, "Web Servers": 1,
        "Enterprise": 25
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

def fetch_url_with_retry(domain):
    candidates = []
    if domain.startswith("http"): candidates.append(domain)
    else:
        candidates.append(f"https://{domain}")
        candidates.append(f"https://www.{domain}")
        candidates.append(f"http://{domain}")

    for url in candidates:
        try:
            print(f"   🕵️ Tentando: {url}...")
            resp = requests.get(url, timeout=10, headers=get_stealth_headers(), allow_redirects=True)
            if resp.status_code == 200: return resp
            if resp.status_code in [403, 406]:
                print(f"   ⛔ Bloqueado (WAF) em {url}")
                time.sleep(1)
        except: pass
    return None

def analyze_domain(domain):
    resp = fetch_url_with_retry(domain)

    if not resp:
        print(f"   ❌ Falha total: {domain}. Passando adiante para enriquecimento de contato.")
        return {
            "tech_list": [],
            "tech_names": [],
            "summary": {},
            "total_score": 0,
            "hosting": "WAF/Bloqueio (Site Inacessível)",
            "site_emails": [],
            "site_socials": []
        }

    try:
        webpage = WebPage(resp.url, resp.text, resp.headers)
        wappa_raw = wappalyzer.analyze_with_versions_and_categories(webpage)
        techs_wappa = process_wappalyzer_result(wappa_raw)
        techs_custom = analyze_advanced_signals(resp.text)
        hosting = get_hosting_provider(wappa_raw, resp.text)
        
        # --- NOVIDADE: Extração de Contatos na mesma viagem ---
        site_emails, site_socials = extract_contact_info(resp.text)
        
        final_dict = {}
        for t in techs_wappa: final_dict[t['name']] = t
        for t in techs_custom: final_dict[t['name']] = t
        
        tech_list = list(final_dict.values())
        total_score = sum([t['score'] for t in tech_list])
        
        summary = {
            "marketing": [t['name'] for t in tech_list if t['category'] in ["Marketing", "CRM", "Ads", "Marketing Automation", "Enterprise Marketing"]],
            "ecommerce": [t['name'] for t in tech_list if t['category'] in ["Ecommerce", "Enterprise Ecommerce"]],
            "cms": [t['name'] for t in tech_list if t['category'] in ["CMS", "CMS Básico"]],
            "analytics": [t['name'] for t in tech_list if t['category'] in ["Analytics"]]
        }

        return {
            "tech_list": tech_list,
            "tech_names": [t['name'] for t in tech_list],
            "summary": summary,
            "total_score": min(total_score, 100),
            "hosting": hosting,
            "site_emails": site_emails,
            "site_socials": site_socials
        }

    except Exception as e:
        print(f"   ⚠️ Erro Análise: {e}")
        return {
            "tech_list": [], "tech_names": [], "summary": {}, "total_score": 0, "hosting": "Erro Processamento",
            "site_emails": [], "site_socials": []
        }

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
            summary = result['summary']
            
            # Recupera os novos dados
            site_emails = result.get('site_emails', [])
            site_socials = result.get('site_socials', [])
            
            if score == 0:
                print(f"   ⚠️ Sem Techs (Host: {host}). Enviando para Agente 3...")
            else:
                print(f"   ✅ Score: {score}/100 | Host: {host} | Emails: {len(site_emails)}")
            
            # Adiciona novos campos no update do DB
            db_payload = {
                "techs": techs, 
                "tech_details": result, 
                "tech_score": score, 
                "tech_date": "NOW",
                "scraped_emails": site_emails,
                "scraped_socials": site_socials
            }
            database.update_techs(domain, db_payload)

            payload = {
                "domain": domain,
                "techs": techs,
                "tech_summary": summary,
                "tech_score": score,
                "hosting": host,
                "chat_id": chat_id,
                "origin_query": origin_query,
                # Passa a bola para o Agente 3
                "site_emails": site_emails,
                "site_socials": site_socials
            }
            publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))

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
