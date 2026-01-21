
"""
AGENTE 2: TECH ANALYST
SalesMachine v4.1 (Corrigido)

Responsabilidades:
- Fetch do site com retry
- Análise Wappalyzer + Custom Signals (LÓGICA ORIGINAL PRESERVADA)
- Extração de emails e redes sociais
- Busca em páginas adicionais (/contato, /sobre, etc)
- Passa HTML comprimido para Agente 3 (evita fetch duplicado)
- Classificação de maturidade da stack (modern/traditional)
"""

import os
import json
import warnings
import requests
import re
import random
import time
import zlib
import base64
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
        def update_techs(domain, data): pass
        @staticmethod
        def save_debug_log(a, b, c, d=None): pass

warnings.filterwarnings("ignore")
load_dotenv()

print("\n🛠️ --- AGENTE 2: TECH ANALYST (V4.1 - Fixed) ---")

# ==============================================================================
# ⚙️ CONFIGURAÇÃO
# ==============================================================================

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DEBUG_CHAT_ID = os.getenv("DEBUG_CHAT_ID", "-1002424609562")

TOPIC_OUTPUT = "topic-enricher"
SUBSCRIPTION_INPUT = "sub-tech-checker"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_OUTPUT)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)

print("📚 Carregando cérebro do Wappalyzer...")
wappalyzer = Wappalyzer.latest()
print("✅ Pronto! Aguardando leads...")

# ==============================================================================
# 🔧 CUSTOM SIGNALS (ORIGINAL - NÃO MEXER)
# ==============================================================================

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
    "Oracle Commerce": {"patterns": [r"oracle\.com", r"atg\.com"], "cat": "Enterprise Ecommerce", "score": 25},
    "Intercom": {"patterns": [r"intercom\.io", r"widget\.intercom\.io"], "cat": "Suporte", "score": 15},
    "Drift": {"patterns": [r"js\.driftt\.com", r"drift\.com"], "cat": "Chat", "score": 10},
    "Pipedrive": {"patterns": [r"pipedrive\.com"], "cat": "CRM", "score": 15},
    "Mailchimp": {"patterns": [r"mailchimp\.com", r"list-manage\.com"], "cat": "Marketing", "score": 8},
    "ActiveCampaign": {"patterns": [r"activehosted\.com", r"activecampaign\.com"], "cat": "Marketing", "score": 12},
    "Segment": {"patterns": [r"segment\.com", r"segment\.io"], "cat": "Analytics", "score": 15},
    "Mixpanel": {"patterns": [r"mixpanel\.com"], "cat": "Analytics", "score": 12},
    "Hotjar": {"patterns": [r"hotjar\.com", r"static\.hotjar\.com"], "cat": "Analytics", "score": 8},
    "Clarity": {"patterns": [r"clarity\.ms"], "cat": "Analytics", "score": 5},
    "Tawk.to": {"patterns": [r"tawk\.to", r"embed\.tawk\.to"], "cat": "Chat", "score": 5},
    "Crisp": {"patterns": [r"crisp\.chat", r"client\.crisp\.chat"], "cat": "Chat", "score": 8},
    "Freshdesk": {"patterns": [r"freshdesk\.com", r"freshworks\.com"], "cat": "Suporte", "score": 12},
    "Zoho": {"patterns": [r"zoho\.com", r"zohocdn\.com"], "cat": "CRM", "score": 10},
    "Calendly": {"patterns": [r"calendly\.com", r"assets\.calendly\.com"], "cat": "Agendamento", "score": 8},
    "Typeform": {"patterns": [r"typeform\.com"], "cat": "Formulários", "score": 8},
    "Stripe": {"patterns": [r"js\.stripe\.com", r"stripe\.com"], "cat": "Pagamentos", "score": 15},
    "PagSeguro": {"patterns": [r"pagseguro\.uol\.com\.br", r"stc\.pagseguro"], "cat": "Pagamentos", "score": 10},
    "MercadoPago": {"patterns": [r"mercadopago\.com", r"secure\.mlstatic\.com"], "cat": "Pagamentos", "score": 10},
    "Cloudflare": {"patterns": [r"cdnjs\.cloudflare\.com", r"cloudflare\.com"], "cat": "CDN", "score": 3},
    "React": {"patterns": [r"react\.js", r"react-dom", r"__REACT"], "cat": "Framework", "score": 5},
    "Vue.js": {"patterns": [r"vue\.js", r"vuejs\.org", r"__VUE"], "cat": "Framework", "score": 5},
    "Angular": {"patterns": [r"angular\.js", r"angular\.io", r"ng-"], "cat": "Framework", "score": 5},
    "Next.js": {"patterns": [r"_next/static", r"__NEXT_DATA__"], "cat": "Framework", "score": 8},
    "Nuxt": {"patterns": [r"_nuxt/", r"__NUXT__"], "cat": "Framework", "score": 8},
    "Laravel": {"patterns": [r"laravel", r"csrf-token"], "cat": "Backend", "score": 5},
    "Django": {"patterns": [r"csrfmiddlewaretoken", r"django"], "cat": "Backend", "score": 5},
}

# ==============================================================================
# 🎯 CLASSIFICAÇÃO DE MATURIDADE
# ==============================================================================

MODERN_STACK_SIGNALS = {
    "HubSpot", "Salesforce", "Segment", "Mixpanel", "Intercom", "Drift",
    "ActiveCampaign", "Pipedrive", "VTEX", "Shopify", "Adobe Experience Cloud",
    "Zendesk", "Google Tag Manager", "Hotjar", "Next.js", "Nuxt", "React", "Vue.js"
}

TRADITIONAL_STACK_SIGNALS = {
    "WordPress", "WooCommerce", "Wix", "Nuvemshop", "JivoChat", "Mailchimp"
}

def classify_stack_maturity(tech_names):
    """Classifica a maturidade da stack tecnológica."""
    modern_count = sum(1 for t in tech_names if t in MODERN_STACK_SIGNALS)
    traditional_count = sum(1 for t in tech_names if t in TRADITIONAL_STACK_SIGNALS)
    
    if modern_count > traditional_count:
        return "modern"
    elif traditional_count > 0:
        return "traditional"
    return "unknown"

# ==============================================================================
# 🌐 HTTP FUNCTIONS
# ==============================================================================

def get_stealth_headers():
    """Headers que simulam navegador real"""
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


def fetch_url_with_retry(domain):
    """Fetch com retry em múltiplas variações de URL"""
    candidates = []
    if domain.startswith("http"):
        candidates.append(domain)
    else:
        candidates.append(f"https://{domain}")
        candidates.append(f"https://www.{domain}")
        candidates.append(f"http://{domain}")

    for url in candidates:
        try:
            print(f"   🕵️ Tentando: {url}...")
            resp = requests.get(url, timeout=10, headers=get_stealth_headers(), allow_redirects=True)
            if resp.status_code == 200:
                return resp
            if resp.status_code in [403, 406]:
                print(f"   ⛔ Bloqueado (WAF) em {url}")
                time.sleep(1)
        except:
            pass
    return None


def fetch_additional_pages(domain, base_url):
    """Busca páginas adicionais para extrair mais informações."""
    additional_html = ""
    pages_to_try = ["/contato", "/sobre", "/quem-somos", "/contact", "/about", "/fale-conosco"]
    
    for page in pages_to_try:
        try:
            url = base_url.rstrip('/') + page
            resp = requests.get(url, timeout=5, headers=get_stealth_headers(), allow_redirects=True)
            if resp.status_code == 200:
                additional_html += f"\n<!-- PAGE: {page} -->\n" + resp.text
                print(f"   📄 Página encontrada: {page}")
        except:
            pass
    
    return additional_html

# ==============================================================================
# 📧 EXTRAÇÃO DE CONTATOS (ORIGINAL)
# ==============================================================================

def extract_contact_info(html_content):
    """Extrai emails e redes sociais do HTML bruto."""
    if not html_content:
        return [], []
    
    # 1. Emails (com filtro de arquivos de imagem/js)
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    found_emails = set(re.findall(email_pattern, html_content))
    
    # Filtra extensões indesejadas e domínios genéricos
    clean_emails = []
    ignored_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.js', '.css', '.svg', '.webp')
    ignored_domains = ('sentry.io', 'wix.com', 'example.com', 'domain.com')
    
    for email in found_emails:
        email_lower = email.lower()
        if not email_lower.endswith(ignored_extensions) and not any(d in email_lower for d in ignored_domains):
            clean_emails.append(email)
            
    # 2. Redes Sociais
    found_socials = []
    patterns = {
        "Instagram": r'https?://(?:www\.)?instagram\.com/[a-zA-Z0-9_.-]+',
        "LinkedIn": r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9_.-]+',
        "Facebook": r'https?://(?:www\.)?facebook\.com/[a-zA-Z0-9_.-]+',
        "WhatsApp": r'https?://(?:api\.whatsapp\.com/send|wa\.me)/\d+'
    }
    
    for net, pat in patterns.items():
        matches = re.findall(pat, html_content)
        if matches:
            found_socials.append(f"{net}: {matches[0]}")
    
    if not any("Instagram" in s for s in found_socials) and "instagram.com" in html_content:
        found_socials.append("Instagram (Link n/d)")
        
    return list(set(clean_emails))[:5], found_socials

# ==============================================================================
# 🔬 ANÁLISE DE TECNOLOGIAS (ORIGINAL - CORRIGIDO)
# ==============================================================================

def analyze_advanced_signals(html_content):
    """Detecta tecnologias usando regex customizado"""
    if not html_content:
        return []
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
    """Identifica o provedor de hosting"""
    html_safe = html.lower() if html else ""
    if "amazonaws" in html_safe or "aws" in html_safe:
        return "AWS (Amazon)"
    if "googleapis" in html_safe or "google cloud" in html_safe:
        return "Google Cloud"
    if "azure" in html_safe:
        return "Microsoft Azure"
    if "cloudflare" in html_safe:
        return "Cloudflare"
    if "akamai" in html_safe:
        return "Akamai (Enterprise)"
    if "locaweb" in html_safe:
        return "Locaweb"
    if "hostgator" in html_safe:
        return "Hostgator"
    if "vercel" in html_safe:
        return "Vercel"
    if "netlify" in html_safe:
        return "Netlify"
    return "Não identificado/Outro"


def process_wappalyzer_result(wappa_raw):
    """
    Processa resultado do Wappalyzer.
    IMPORTANTE: Usa .lower() para comparar categorias (case-insensitive)
    """
    processed = []
    
    # Mapa de scores por categoria (tudo em lowercase para comparação)
    cat_score_map = {
        "ecommerce": 10,
        "crm": 20,
        "marketing automation": 15,
        "analytics": 5,
        "advertising": 5,
        "cms": 5,
        "web servers": 1,
        "programming languages": 2,
        "javascript frameworks": 3,
        "web frameworks": 3,
        "databases": 5,
        "caching": 3,
        "paas": 5,
        "hosting": 3,
        "cdn": 3,
        "tag managers": 5,
        "live chat": 5,
        "widgets": 3,
        "email": 5,
        "marketing": 8,
        "payment processors": 10,
        "security": 3,
    }
    
    for name, data in wappa_raw.items():
        cats = data.get('categories', [])
        main_cat = cats[0] if cats else "Outros"
        
        # Calcula score baseado nas categorias
        score = 1
        for c in cats:
            c_lower = c.lower()
            if c_lower in cat_score_map:
                score = max(score, cat_score_map[c_lower])
        
        processed.append({
            "name": name,
            "category": main_cat,
            "score": score,
            "source": "Wappalyzer"
        })
    
    return processed

# ==============================================================================
# 🗜️ COMPRESSÃO HTML
# ==============================================================================

def compress_html(html_content):
    """Comprime o HTML para economizar espaço no payload."""
    if not html_content:
        return ""
    try:
        html_limited = html_content[:500000]
        compressed = zlib.compress(html_limited.encode('utf-8'), level=9)
        return base64.b64encode(compressed).decode('ascii')
    except Exception as e:
        print(f"⚠️ Erro ao comprimir HTML: {e}")
        return ""

# ==============================================================================
# 🎯 ANÁLISE PRINCIPAL (LÓGICA ORIGINAL PRESERVADA)
# ==============================================================================

def analyze_domain(domain):
    """Análise completa do domínio"""
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
            "site_socials": [],
            "html_compressed": "",
            "stack_maturity": "unknown",
            "final_url": ""
        }

    try:
        # ==== ANÁLISE WAPPALYZER (ORIGINAL) ====
        webpage = WebPage(resp.url, resp.text, resp.headers)
        wappa_raw = wappalyzer.analyze_with_versions_and_categories(webpage)
        techs_wappa = process_wappalyzer_result(wappa_raw)
        
        # ==== CUSTOM SIGNALS (ORIGINAL) ====
        techs_custom = analyze_advanced_signals(resp.text)
        
        # ==== HOSTING (ORIGINAL) ====
        hosting = get_hosting_provider(wappa_raw, resp.text)
        
        # ==== PÁGINAS ADICIONAIS (NOVO) ====
        print(f"   🔍 Buscando páginas adicionais...")
        additional_html = fetch_additional_pages(domain, resp.url)
        full_html = resp.text + additional_html
        
        # ==== EXTRAÇÃO DE CONTATOS (ORIGINAL) ====
        site_emails, site_socials = extract_contact_info(full_html)
        
        # ==== CONSOLIDAÇÃO (ORIGINAL) ====
        final_dict = {}
        for t in techs_wappa:
            final_dict[t['name']] = t
        for t in techs_custom:
            final_dict[t['name']] = t
        
        tech_list = list(final_dict.values())
        total_score = sum([t['score'] for t in tech_list])
        
        # ==== CLASSIFICAÇÃO DE MATURIDADE (NOVO) ====
        tech_names = [t['name'] for t in tech_list]
        stack_maturity = classify_stack_maturity(tech_names)
        
        # ==== SUMÁRIO (ORIGINAL - com categorias corretas) ====
        # Usa lowercase para comparação consistente
        summary = {
            "marketing": [t['name'] for t in tech_list if t['category'].lower() in ["marketing", "crm", "ads", "marketing automation", "enterprise marketing", "crm/marketing"]],
            "ecommerce": [t['name'] for t in tech_list if t['category'].lower() in ["ecommerce", "enterprise ecommerce"]],
            "cms": [t['name'] for t in tech_list if t['category'].lower() in ["cms", "cms básico"]],
            "analytics": [t['name'] for t in tech_list if t['category'].lower() in ["analytics", "tag managers"]]
        }

        return {
            "tech_list": tech_list,
            "tech_names": tech_names,
            "summary": summary,
            "total_score": min(total_score, 100),
            "hosting": hosting,
            "site_emails": site_emails,
            "site_socials": site_socials,
            "html_compressed": compress_html(full_html),
            "stack_maturity": stack_maturity,
            "final_url": resp.url
        }

    except Exception as e:
        print(f"   ⚠️ Erro Análise: {e}")
        import traceback
        traceback.print_exc()
        return {
            "tech_list": [],
            "tech_names": [],
            "summary": {},
            "total_score": 0,
            "hosting": "Erro Processamento",
            "site_emails": [],
            "site_socials": [],
            "html_compressed": "",
            "stack_maturity": "unknown",
            "final_url": ""
        }

# ==============================================================================
# 📨 CALLBACK PRINCIPAL
# ==============================================================================

def callback(message):
    """Callback principal do Pub/Sub"""
    try:
        data = json.loads(message.data.decode("utf-8"))
        domain = data.get("domain")
        chat_id = data.get("chat_id")
        origin_query = data.get("origin_query")
        context_data = data.get("context_data", {})
        
        print(f"\n📨 RECEBIDO: {domain}")
        
        result = analyze_domain(domain)
        
        if result:
            score = result['total_score']
            host = result['hosting']
            techs = result['tech_names']
            summary = result['summary']
            site_emails = result.get('site_emails', [])
            site_socials = result.get('site_socials', [])
            stack_maturity = result.get('stack_maturity', 'unknown')
            
            if score == 0:
                print(f"   ⚠️ Sem Techs (Host: {host}). Enviando para Agente 3...")
            else:
                print(f"   ✅ Score: {score}/100 | Host: {host} | Techs: {len(techs)} | Emails: {len(site_emails)}")
            
            # Salva no banco
            db_payload = {
                "techs": techs,
                "tech_details": result['tech_list'],
                "tech_score": score,
                "tech_date": "NOW",
                "scraped_emails": site_emails,
                "scraped_socials": site_socials,
                "hosting": host,
                "stack_maturity": stack_maturity
            }
            database.update_techs(domain, db_payload)

            # Payload para Agente 3
            payload = {
                "domain": domain,
                "techs": techs,
                "tech_summary": summary,
                "tech_score": score,
                "hosting": host,
                "chat_id": chat_id,
                "origin_query": origin_query,
                "context_data": context_data,
                "site_emails": site_emails,
                "site_socials": site_socials,
                "html_compressed": result.get('html_compressed', ''),
                "stack_maturity": stack_maturity,
                "final_url": result.get('final_url', '')
            }
            
            publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
            print(f"   📤 Enviado para Agente 3 | Techs: {techs[:5]}...")

        message.ack()
        
    except Exception as e:
        print(f"🔥 Erro Worker: {e}")
        import traceback
        traceback.print_exc()
        message.ack()

# ==============================================================================
# 🚀 MAIN
# ==============================================================================

if __name__ == "__main__":
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    print(f"🛠️ Agente 2 (V4.1 - Fixed) ouvindo...")
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control).result()
        except KeyboardInterrupt:
            print("\n👋 Agente 2 finalizado.")
