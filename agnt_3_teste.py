"""
AGENTE 3: ENRICHER PREMIUM
SalesMachine v4.8

Correções v4.8:
- Enriquece sócios para TODOS os portes (não só PME)
- Debug adicional na busca de CNPJ
- Mantém correções de DataStone e deduplicação
"""

import os
import json
import requests
import datetime
import traceback
import re
import zlib
import base64
from google.cloud import pubsub_v1
from google.cloud import firestore
from dotenv import load_dotenv

# --- Banco ---
try:
    import database
    print("✅ [Agente 3] Módulo database carregado.")
except ImportError:
    print("⚠️ [Agente 3] ERRO: database.py não encontrado.")
    class database:
        @staticmethod
        def update_enrichment(domain, data): pass
        @staticmethod
        def get_cnpj_cache(cnpj): return None
        @staticmethod
        def save_cnpj_cache(cnpj, data): pass

load_dotenv()
print("\n💎 --- AGENTE 3: ENRICHER (V4.8 - Sócios Universal) ---")

# ==============================================================================
# ⚙️ CONFIGURAÇÃO
# ==============================================================================

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DEBUG_CHAT_ID = os.getenv("DEBUG_CHAT_ID")

# API Keys
CRUST_API_KEY = os.getenv("CRUST_API_KEY")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
LUSHA_API_KEY = os.getenv("LUSHA_API_KEY")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
DATA_STONE_API_KEY = os.getenv("DATA_STONE_API_KEY")

# URLs
CRUST_BASE_URL = "https://api.crustdata.com"
DATA_STONE_BASE_URL = "https://docs.datastone.com.br/_mock/api"

# Pub/Sub
SUBSCRIPTION_INPUT = "sub-enricher-worker"
TOPIC_CLOSER = "topic-closer-hubspot"
TOPIC_COPY = "topic-copy-generator"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path_closer = publisher.topic_path(PROJECT_ID, TOPIC_CLOSER)
topic_path_copy = publisher.topic_path(PROJECT_ID, TOPIC_COPY)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)

# Firestore
db_firestore = firestore.Client(project=PROJECT_ID)

# Limites
MAX_SERPER_CALLS = 5

# ==============================================================================
# 🛠️ UTILITÁRIOS
# ==============================================================================

def clean_markdown(text):
    """Remove caracteres que quebram o Markdown"""
    if not text:
        return ""
    return str(text).replace("_", " ").replace("*", " ").replace("`", "'").replace("[", "(").replace("]", ")")


def parse_date_ym(date_str):
    """Converte data para formato YYYY-MM"""
    if not date_str:
        return None
    try:
        return datetime.datetime.fromisoformat(date_str.replace("Z", "")).strftime("%Y-%m")
    except:
        return date_str


def decompress_html(compressed_str):
    """Descomprime HTML que veio do Agente 2"""
    if not compressed_str:
        return ""
    try:
        compressed_bytes = base64.b64decode(compressed_str)
        html_bytes = zlib.decompress(compressed_bytes)
        return html_bytes.decode('utf-8')
    except:
        return ""


def format_person_profile_full(person):
    """
    Gera o card rico do funcionário (FORMATO ORIGINAL DO CRUSTDATA)
    Inclui histórico de empregos
    """
    name = clean_markdown(person.get("full_name") or person.get("name"))
    linkedin = person.get("linkedin_profile_url") or person.get("linkedin_url")
    location = clean_markdown(person.get("city") or person.get("location_city") or person.get("location"))
    email = person.get("email")
    phone = person.get("phone")
    faixa_etaria = person.get("faixa_etaria")

    employers = person.get("employer") or []
    if not isinstance(employers, list):
        employers = []

    current = None
    if employers:
        for e in employers:
            if e.get("is_default"):
                current = e
                break
        if current is None and employers:
            current = sorted(employers, key=lambda e: e.get("start_date") or "", reverse=True)[0]

    lines = []
    
    # 1. Header
    if current:
        title = clean_markdown(current.get("title"))
        line1 = f"👤 *{name}* — {title}"
        if location:
            line1 += f" ({location})"
    else:
        title = person.get("title") or person.get("cargo") or ""
        if title:
            line1 = f"👤 *{name}* — {clean_markdown(title)}"
        else:
            line1 = f"👤 *{name or 'Contato'}*"
        if location:
            line1 += f" ({location})"
    lines.append(line1)

    # 2. Faixa etária (se vier do BrasilAPI)
    if faixa_etaria:
        lines.append(f"   📅 {faixa_etaria}")

    # 3. Descrição/Start date
    if current:
        start = parse_date_ym(current.get("start_date"))
        base = f"Start: {start}" if start else "Atual"
        desc = current.get("description") or person.get("headline") or person.get("summary")
        if desc:
            clean_desc = clean_markdown(desc.strip().replace("\n", " "))
            if len(clean_desc) > 120:
                clean_desc = clean_desc[:117] + "..."
            lines.append(f"   _{base} | {clean_desc}_")
        else:
            lines.append(f"   _{base}_")

    # 4. Histórico de empregos
    if employers:
        sorted_emp = sorted(employers, key=lambda e: e.get("start_date") or "", reverse=True)
        hist_lines = []
        count = 0
        for e in sorted_emp:
            if current and e is current:
                continue
            s = parse_date_ym(e.get("start_date"))
            end = parse_date_ym(e.get("end_date")) or "atual"
            tit = clean_markdown(e.get("title"))
            cmp = clean_markdown(e.get("company_name"))
            hist_lines.append(f"   • {s if s else '?'} - {end}: {tit} @ {cmp}")
            count += 1
            if count >= 3:
                break
        
        if hist_lines:
            lines.append("   📜 *Histórico:*")
            lines.extend(hist_lines)

    # 5. Contatos
    if email:
        lines.append(f"   📧 {clean_markdown(email)}")
    if phone:
        lines.append(f"   📞 {clean_markdown(phone)}")
    if linkedin:
        lines.append(f"   🔗 [LinkedIn]({linkedin})")

    return "\n".join(lines)


# ==============================================================================
# 📡 TELEGRAM
# ==============================================================================

def send_telegram(chat_id, text, reply_markup=None):
    """Envia mensagem no Telegram"""
    if not TELEGRAM_TOKEN or not chat_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("result", {}).get("message_id")
        else:
            # Tenta sem Markdown se falhar
            payload["parse_mode"] = None
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return resp.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"⚠️ Erro Telegram: {e}")
    return None


def send_telegram_preview(chat_id, text, domain):
    """Envia preview com botões de ação"""
    keyboard = {
        "inline_keyboard": [[
            {"text": "👥 Enriquecer Pessoas", "callback_data": f"ENRICH:{domain}"},
            {"text": "🗑 Descartar", "callback_data": f"DISCARD:{domain}"}
        ]]
    }
    return send_telegram(chat_id, text, keyboard)


def edit_msg_final(chat_id, msg_id, text):
    """Edita mensagem existente"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    try:
        payload = {
            "chat_id": chat_id,
            "message_id": msg_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        r = requests.post(url, json=payload)
        if r.status_code != 200:
            payload["parse_mode"] = None
            requests.post(url, json=payload)
    except Exception as e:
        print(f"⚠️ Falha envio Telegram: {e}")


def send_new_message_with_copies_button(chat_id, text, domain):
    """
    Envia NOVA mensagem com botão de gerar copies
    NÃO edita a mensagem anterior
    """
    keyboard = {
        "inline_keyboard": [[
            {"text": "🚀 Gerar Copies", "callback_data": f"COPIES:{domain}"},
            {"text": "📋 Ver no HubSpot", "callback_data": f"HUBSPOT:{domain}"}
        ]]
    }
    return send_telegram(chat_id, text, keyboard)


# ==============================================================================
# 🔍 EXTRAÇÃO DE CNPJ
# ==============================================================================

def extract_cnpj_from_html(html_content):
    """Extrai CNPJ do HTML usando regex"""
    if not html_content:
        return None
    
    patterns = [
        r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}',
        r'\d{2}\s?\.\s?\d{3}\s?\.\s?\d{3}\s?/\s?\d{4}\s?-\s?\d{2}',
        r'CNPJ[:\s]*(\d{2}\.?\d{3}\.?\d{3}/?\.?\d{4}-?\d{2})',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        if matches:
            for match in matches:
                cnpj_clean = "".join(filter(str.isdigit, str(match)))
                if len(cnpj_clean) == 14:
                    print(f"   📋 CNPJ encontrado: {cnpj_clean[:8]}...")
                    return cnpj_clean
    return None


def search_cnpj_serper(company_name, domain):
    """Busca CNPJ via Serper quando não encontrou no site"""
    if not SERPER_API_KEY or not company_name:
        return None
    
    try:
        query = f"{company_name} CNPJ site:cnpj.info OR site:consultasocio.com"
        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
        
        resp = requests.post(url, headers=headers, json={"q": query, "num": 5, "gl": "br"}, timeout=10)
        
        if resp.status_code == 200:
            for result in resp.json().get("organic", []):
                snippet = result.get("snippet", "") + result.get("title", "")
                cnpj = extract_cnpj_from_html(snippet)
                if cnpj:
                    print(f"   📋 CNPJ via Serper: {cnpj[:8]}...")
                    return cnpj
    except Exception as e:
        print(f"   ⚠️ Serper CNPJ erro: {e}")
    return None


# ==============================================================================
# 🇧🇷 BRASIL API
# ==============================================================================

def fetch_brasil_api(cnpj):
    """Consulta BrasilAPI para dados do CNPJ"""
    if not cnpj:
        return None
    
    cnpj_limpo = "".join(filter(str.isdigit, str(cnpj)))
    if len(cnpj_limpo) != 14:
        return None
    
    # Cache
    cached = database.get_cnpj_cache(cnpj_limpo)
    if cached:
        print(f"   📦 CNPJ Cache hit")
        return cached
    
    try:
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"
        resp = requests.get(url, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json()
            database.save_cnpj_cache(cnpj_limpo, data)
            print(f"   ✅ BrasilAPI: {data.get('razao_social', 'OK')[:30]}...")
            return data
        elif resp.status_code == 404:
            print(f"   ⚠️ CNPJ não encontrado na Receita")
    except Exception as e:
        print(f"   ⚠️ BrasilAPI erro: {e}")
    return None


def extract_socios_from_brasil_api(brasil_data):
    """Extrai sócios do QSA da BrasilAPI"""
    if not brasil_data:
        return []
    
    qsa = brasil_data.get("qsa", [])
    socios = []
    
    # Prioriza sócio-administrador
    sorted_qsa = sorted(qsa, key=lambda x: (
        0 if "ADMINISTRADOR" in str(x.get("qualificacao_socio", "")).upper() else 1
    ))
    
    for socio in sorted_qsa[:5]:
        nome = socio.get("nome_socio", "")
        if nome and nome.strip():
            socios.append({
                "nome": nome.title(),
                "qualificacao": socio.get("qualificacao_socio", "Sócio"),
                "faixa_etaria": socio.get("faixa_etaria"),
                "data_entrada": socio.get("data_entrada_sociedade"),
                "cpf": socio.get("cpf_cnpj_socio")  # CPF do sócio (se disponível)
            })
    return socios


# ==============================================================================
# 📊 DATA STONE API (Enriquecimento de Pessoa Física)
# ==============================================================================

def fetch_datastone_person_by_name(name, uf=None):
    """
    Consulta DataStone API para buscar dados de pessoa física por NOME
    Usa o endpoint /persons/search
    Retorna email e celular se encontrar
    """
    if not DATA_STONE_API_KEY or not name:
        return None
    
    # Só converte para uppercase (como no exemplo da API)
    nome_busca = name.upper()
    
    try:
        url = f"{DATA_STONE_BASE_URL}/persons/search"
        
        # Header conforme documentação: Authorization: YOUR_API_KEY (sem Bearer)
        headers = {
            "Authorization": DATA_STONE_API_KEY
        }
        params = {
            "name": nome_busca
        }
        # Adiciona UF se disponível para refinar a busca
        if uf:
            params["uf"] = uf.upper()
        
        print(f"   🔗 DataStone URL: {url}?name={nome_busca[:20]}...")
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        
        # Debug em caso de erro
        if resp.status_code != 200:
            print(f"   ⚠️ DataStone: Status {resp.status_code} | {resp.text[:100] if resp.text else 'vazio'}")
            return None
        
        # Verifica se tem conteúdo
        if not resp.text or resp.text.strip() == "":
            print(f"   ⚠️ DataStone: Resposta vazia")
            return None
        
        data = resp.json()
        
        # A resposta pode ser uma lista ou um objeto
        if isinstance(data, list) and len(data) > 0:
            data = data[0]  # Pega o primeiro resultado
        
        if not data:
            print(f"   ⚠️ DataStone: Nenhum resultado para {nome_limpo[:20]}...")
            return None
        
        result = {
            "name": data.get("name"),
            "cpf": data.get("cpf")
        }
        
        # Extrai primeiro email
        emails = data.get("emails", [])
        if emails and isinstance(emails, list) and len(emails) > 0:
            if isinstance(emails[0], dict):
                result["email"] = emails[0].get("email")
            else:
                result["email"] = emails[0]
        
        # Extrai primeiro celular
        phones = data.get("mobile_phones", [])
        if phones and isinstance(phones, list) and len(phones) > 0:
            if isinstance(phones[0], dict):
                result["phone"] = phones[0].get("phone") or phones[0].get("number")
            else:
                result["phone"] = phones[0]
        
        print(f"   📱 DataStone: {result.get('name', 'OK')[:20]}... | Email: {'✅' if result.get('email') else '❌'} | Tel: {'✅' if result.get('phone') else '❌'}")
        return result
        
    except json.JSONDecodeError as e:
        print(f"   ⚠️ DataStone: Resposta não é JSON válido")
    except Exception as e:
        print(f"   ⚠️ DataStone erro: {e}")
    return None


# ==============================================================================
# 🦀 CRUST DATA (LÓGICA ORIGINAL - RICA)
# ==============================================================================

def enrich_company_basic(domain):
    """Busca dados da empresa no CrustData"""
    if not CRUST_API_KEY:
        return None
    try:
        url = f"{CRUST_BASE_URL}/screener/company"
        resp = requests.get(
            url,
            headers={"Authorization": f"Token {CRUST_API_KEY}"},
            params={"company_domain": domain},
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            return data[0] if isinstance(data, list) and len(data) > 0 else None
    except Exception as e:
        print(f"   ⚠️ CrustData Company erro: {e}")
    return None


def get_decision_makers_by_id(company_id):
    """
    Busca decision makers pelo ID da empresa no CrustData
    Retorna lista vazia se falhar, nunca None
    """
    if not company_id or not CRUST_API_KEY:
        return []
    try:
        url = f"{CRUST_BASE_URL}/screener/company"
        params = {"company_id": str(company_id), "fields": "decision_makers"}
        resp = requests.get(
            url,
            headers={"Authorization": f"Token {CRUST_API_KEY}"},
            params=params,
            timeout=20
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data:
                return data[0].get("decision_makers") or []
            elif isinstance(data, dict):
                return data.get("decision_makers") or []
    except Exception as e:
        print(f"   ⚠️ CrustData DMs erro: {e}")
    return []


def search_people_robust(company_key, titles_list=None, limit=5):
    """
    Busca pessoas no CrustData com filtros de título
    LÓGICA ORIGINAL - retorna dados ricos
    """
    if not company_key or not CRUST_API_KEY:
        return []
    
    url = f"{CRUST_BASE_URL}/screener/person/search/"
    headers = {"Authorization": f"Token {CRUST_API_KEY}", "Content-Type": "application/json"}
    
    filters = [{"filter_type": "CURRENT_COMPANY", "type": "in", "value": [company_key]}]
    if titles_list:
        filters.append({"filter_type": "CURRENT_TITLE", "type": "contains_any", "value": titles_list})
    
    try:
        resp = requests.post(url, headers=headers, json={"filters": filters, "limit": limit}, timeout=25)
        if resp.status_code == 200:
            data = resp.json()
            res = data.get("results") or data.get("profiles")
            return res if res else []
    except Exception as e:
        print(f"   ⚠️ CrustData Search erro: {e}")
    return []


# ==============================================================================
# 🚀 APOLLO API (FALLBACK)
# ==============================================================================

def apollo_organization_search(domain):
    """Busca dados da empresa no Apollo"""
    if not APOLLO_API_KEY:
        return None
    try:
        url = "https://api.apollo.io/v1/organizations/search"
        payload = {
            "api_key": APOLLO_API_KEY,
            "q_organization_domains": domain,
            "page": 1,
            "per_page": 1
        }
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            orgs = resp.json().get("organizations", [])
            if orgs:
                org = orgs[0]
                return {
                    "name": org.get("name"),
                    "linkedin_url": org.get("linkedin_url"),
                    "phone": org.get("phone"),
                    "employee_count": org.get("estimated_num_employees"),
                    "industry": org.get("industry"),
                    "founded_year": org.get("founded_year")
                }
    except Exception as e:
        print(f"   ⚠️ Apollo Org erro: {e}")
    return None


def apollo_people_search(domain, titles=None, limit=5):
    """Busca pessoas no Apollo"""
    if not APOLLO_API_KEY:
        return []
    try:
        url = "https://api.apollo.io/v1/mixed_people/search"
        payload = {
            "api_key": APOLLO_API_KEY,
            "q_organization_domains": domain,
            "page": 1,
            "per_page": limit
        }
        if titles:
            payload["person_titles"] = titles
        
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            people = []
            for p in resp.json().get("people", []):
                people.append({
                    "name": p.get("name"),
                    "full_name": p.get("name"),
                    "title": p.get("title"),
                    "linkedin_url": p.get("linkedin_url"),
                    "linkedin_profile_url": p.get("linkedin_url"),
                    "email": p.get("email"),
                    "phone": p.get("phone_numbers", [{}])[0].get("sanitized_number") if p.get("phone_numbers") else None,
                    "source": "apollo"
                })
            return people
    except Exception as e:
        print(f"   ⚠️ Apollo People erro: {e}")
    return []


# ==============================================================================
# 🔮 LUSHA API (FALLBACK)
# ==============================================================================

def lusha_people_search(domain, titles=None, limit=5):
    """Busca pessoas no Lusha"""
    if not LUSHA_API_KEY:
        return []
    try:
        url = "https://api.lusha.com/prospecting/contact/search"
        headers = {"api_key": LUSHA_API_KEY, "Content-Type": "application/json"}
        
        filters = {
            "companies": {"include": {"fqdn": [domain]}},
            "contacts": {"include": {}}
        }
        if titles:
            filters["contacts"]["include"]["jobTitles"] = titles
        
        payload = {"filters": filters, "pages": {"page": 0, "size": limit}, "includePartialContact": True}
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        
        if resp.status_code == 200:
            people = []
            for c in resp.json().get("contacts", []):
                people.append({
                    "name": c.get("name"),
                    "full_name": c.get("name"),
                    "title": c.get("jobTitle"),
                    "linkedin_url": c.get("linkedinUrl"),
                    "linkedin_profile_url": c.get("linkedinUrl"),
                    "email": c.get("email") if c.get("hasEmails") else None,
                    "phone": c.get("phone") if c.get("hasPhones") else None,
                    "source": "lusha"
                })
            return people
    except Exception as e:
        print(f"   ⚠️ Lusha erro: {e}")
    return []


# ==============================================================================
# 🔍 SERPER (LinkedIn Search para sócios)
# ==============================================================================

def search_linkedin_serper(name, company_name):
    """Busca perfil LinkedIn via Serper"""
    if not SERPER_API_KEY or not name:
        return None
    try:
        query = f'"{name}" {company_name} site:linkedin.com/in'
        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
        
        resp = requests.post(url, headers=headers, json={"q": query, "num": 3, "gl": "br"}, timeout=10)
        
        if resp.status_code == 200:
            for result in resp.json().get("organic", []):
                link = result.get("link", "")
                if "linkedin.com/in/" in link:
                    print(f"   🔗 LinkedIn: {link[:50]}...")
                    return link
    except Exception as e:
        print(f"   ⚠️ Serper LinkedIn erro: {e}")
    return None


# ==============================================================================
# 📊 CLASSIFICAÇÃO DE PORTE
# ==============================================================================

def classify_porte(brasil_data, employee_count):
    """Classifica empresa como PME ou Enterprise"""
    if brasil_data:
        porte = brasil_data.get("porte", "").upper()
        if porte in ["MEI", "MICRO EMPRESA", "MICROEMPRESA", "EMPRESA DE PEQUENO PORTE"]:
            return "pme"
        if porte in ["DEMAIS", "GRANDE", "MEDIO"]:
            return "enterprise"
    
    if employee_count:
        try:
            if int(employee_count) < 25:
                return "pme"
            return "enterprise"
        except:
            pass
    
    return "enterprise"


# ==============================================================================
# 🎯 PARTE 1: PREVIEW DO LEAD
# ==============================================================================

def process_new_lead_part1(data):
    """
    PARTE 1: Recebe lead do Agente 2, busca dados da empresa e envia preview
    """
    domain = data.get("domain")
    chat_id = data.get("chat_id")
    techs = data.get("techs", [])
    tech_summary = data.get("tech_summary", {})
    tech_score = data.get("tech_score", 0)
    context_data = data.get("context_data", {})
    html_compressed = data.get("html_compressed", "")
    site_emails = data.get("site_emails", [])
    site_socials = data.get("site_socials", [])
    
    print(f"\n🔵 [Parte 1] Analisando Empresa: {domain}")
    
    # 1. Descomprime HTML
    html_content = decompress_html(html_compressed)
    if html_content:
        print(f"   📄 HTML descomprimido: {len(html_content)} chars")
    
    # 2. Extrai CNPJ (PARA TODOS - PME E ENTERPRISE)
    cnpj = extract_cnpj_from_html(html_content)
    if not cnpj:
        company_name = context_data.get("name", domain.split(".")[0])
        print(f"   🔍 Buscando CNPJ via Serper para: {company_name}")
        cnpj = search_cnpj_serper(company_name, domain)
    
    if cnpj:
        print(f"   ✅ CNPJ: {cnpj[:8]}...")
    else:
        print(f"   ⚠️ CNPJ não encontrado")
    
    # 3. Consulta BrasilAPI (PARA TODOS)
    brasil_data = None
    socios = []
    if cnpj:
        brasil_data = fetch_brasil_api(cnpj)
        if brasil_data:
            socios = extract_socios_from_brasil_api(brasil_data)
    
    # 4. Busca dados no CrustData (LÓGICA ORIGINAL)
    print(f"   🦀 Buscando no CrustData...")
    comp_info = enrich_company_basic(domain)
    
    # 5. Monta payload da empresa
    if comp_info:
        rev_low = comp_info.get("estimated_revenue_lower_bound_usd")
        rev_high = comp_info.get("estimated_revenue_higher_bound_usd")
        if rev_low is not None and rev_high is not None:
            rev = f"${rev_low:,.0f} - ${rev_high:,.0f}"
        else:
            rev = "N/D"
        
        comp_payload = {
            "name": comp_info.get("company_name", domain),
            "id": comp_info.get("company_id"),
            "domain": comp_info.get("company_website_domain", domain),
            "hq": comp_info.get("headquarters"),
            "employees": comp_info.get("employee_count_range"),
            "revenue": rev,
            "description": comp_info.get("linkedin_company_description", ""),
            "linkedin_url": comp_info.get("linkedin_url")
        }
    else:
        # Fallback para Apollo
        apollo_org = apollo_organization_search(domain)
        if apollo_org:
            comp_payload = {
                "name": apollo_org.get("name", domain),
                "employees": apollo_org.get("employee_count"),
                "linkedin_url": apollo_org.get("linkedin_url"),
                "revenue": "N/D"
            }
        else:
            comp_payload = {"name": domain, "revenue": "N/D"}
    
    # 6. Classifica porte
    employee_count = comp_payload.get("employees")
    porte = classify_porte(brasil_data, employee_count)
    
    # 7. Monta mensagem de preview
    msg = f"🔎 *ANÁLISE DE LEAD (Revisão)*\n"
    msg += f"🏢 *{clean_markdown(comp_payload.get('name'))}*"
    msg += f" ({porte.upper()})\n"
    
    if comp_info:
        msg += f"📍 {clean_markdown(comp_payload.get('hq', 'N/D'))} | 👥 {comp_payload.get('employees', 'N/D')}\n"
        msg += f"💰 Rev: {comp_payload.get('revenue')}\n"
        if comp_payload.get('description'):
            msg += f"📝 _{clean_markdown(comp_payload['description'][:150])}..._\n"
    
    # 8. Dados do CNPJ (PARA TODOS)
    if cnpj:
        cnpj_fmt = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
        msg += f"\n📋 *CNPJ:* {cnpj_fmt}\n"
        if brasil_data:
            tel_cnpj = brasil_data.get("ddd_telefone_1")
            email_cnpj = brasil_data.get("email")
            if tel_cnpj:
                msg += f"📞 Cartão CNPJ: {clean_markdown(tel_cnpj)}\n"
            if email_cnpj:
                msg += f"📧 Cartão CNPJ: {clean_markdown(email_cnpj)}\n"
            
            # ⭐ MOSTRA SÓCIOS COM NOME COMPLETO E FAIXA ETÁRIA
            if socios:
                msg += f"\n👥 *Sócios ({len(socios)}):*\n"
                for s in socios:
                    nome = clean_markdown(s.get('nome', ''))
                    qualif = clean_markdown(s.get('qualificacao', 'Sócio'))
                    faixa = s.get('faixa_etaria')
                    
                    linha = f"   • {nome}"
                    if qualif:
                        linha += f" - {qualif}"
                    if faixa:
                        linha += f" ({faixa})"
                    msg += linha + "\n"
    
    msg += "\n----------------\n"
    
    # 9. Stack tecnológica
    if tech_summary:
        if tech_summary.get('marketing'):
            msg += f"📢 Mkt: {', '.join(tech_summary['marketing'][:4])}\n"
        if tech_summary.get('cms'):
            msg += f"📝 CMS: {', '.join(tech_summary['cms'][:3])}\n"
        if tech_summary.get('analytics'):
            msg += f"📈 Data: {', '.join(tech_summary['analytics'][:3])}\n"
        others = [t for t in techs if t not in set(
            tech_summary.get('marketing', []) + 
            tech_summary.get('cms', []) + 
            tech_summary.get('analytics', [])
        )]
        if others:
            msg += f"⚙️ Infra: {', '.join(others[:5])}\n"
    elif techs:
        msg += f"🛠 Stack: {', '.join(techs[:6])}\n"
    
    msg += "----------------\n"
    
    # 10. Score preliminar
    enrich_points = 20 if comp_info else 10
    cnpj_points = 10 if cnpj else 0
    pre_score = int(tech_score * 0.5) + enrich_points + cnpj_points
    msg += f"📊 Score Preliminar: {pre_score}/100"
    
    # 11. Salva no Firestore (INCLUI A MENSAGEM DE PREVIEW PARA CONCATENAÇÃO)
    db_data = {
        "tech_score": tech_score,
        "preliminary_score": pre_score,
        "crust_company": comp_payload,
        "tech_data": techs,
        "tech_summary": tech_summary,
        "cnpj": cnpj,
        "brasil_data": brasil_data,
        "socios": socios,
        "porte": porte,
        "site_emails": site_emails,
        "site_socials": site_socials,
        "status": "WAITING_DECISION",
        "contacts_found": 0,
        "last_update": datetime.datetime.now(),
        "chat_id": chat_id,
        "preview_message": msg  # ⭐ SALVA A MENSAGEM PARA CONCATENAR DEPOIS
    }
    database.update_enrichment(domain, db_data)
    
    # 12. Envia preview com botões
    send_telegram_preview(chat_id, msg, domain)
    print(f"   ✅ Preview enviado | Porte: {porte} | CNPJ: {'✅' if cnpj else '❌'}")


# ==============================================================================
# 🎯 PARTE 2: ENRIQUECIMENTO DE PESSOAS
# ==============================================================================

def process_enrich_command_part2(data):
    """
    PARTE 2: Quando o usuário clica em "Enriquecer Pessoas"
    Busca contatos usando CrustData (prioridade) + Apollo/Lusha (fallback)
    """
    domain = data.get("domain")
    chat_id = data.get("chat_id")
    msg_id = data.get("message_id")

    print(f"\n🟢 [Parte 2] Enriquecendo Pessoas: {domain}")

    try:
        # 1. Busca lead no Firestore
        doc_ref = db_firestore.collection("leads_b2b").document(domain)
        doc = doc_ref.get()
        if not doc.exists:
            edit_msg_final(chat_id, msg_id, "❌ Erro: Lead expirou ou não existe.")
            return

        ld = doc.to_dict()
        comp_info = ld.get("crust_company", {})
        techs = ld.get("tech_data", [])
        tech_summary = ld.get("tech_summary", {})
        pre_score = ld.get("preliminary_score", 0)
        cnpj = ld.get("cnpj")
        brasil_data = ld.get("brasil_data", {})
        socios = ld.get("socios", [])
        porte = ld.get("porte", "enterprise")
        site_emails = ld.get("site_emails", [])
        preview_message = ld.get("preview_message", "")  # ⭐ RECUPERA A MENSAGEM DE PREVIEW

        cid = comp_info.get("id")
        cdom = comp_info.get("domain") or domain

        final_people = []
        seen_ids = set()
        seen_names = set()  # ⭐ Adiciona set de nomes para deduplicação extra
        
        def is_duplicate(person):
            """Verifica se pessoa já está na lista (por ID ou nome similar)"""
            pid = person.get("person_id") or person.get("linkedin_profile_url")
            name = (person.get("full_name") or person.get("name") or "").lower().strip()
            
            # Checa por ID
            if pid and pid in seen_ids:
                return True
            
            # Checa por nome (normalizado - só primeiro e último nome)
            if name:
                parts = name.split()
                if len(parts) >= 2:
                    # Compara primeiro + último nome
                    name_key = f"{parts[0]} {parts[-1]}"
                else:
                    name_key = name
                
                if name_key in seen_names:
                    return True
                
                # Adiciona nos sets
                seen_names.add(name_key)
            
            if pid:
                seen_ids.add(pid)
            
            return False

        # 2. CRUSTDATA - Decision Makers via ID (LÓGICA ORIGINAL)
        if cid:
            print("   ↳ CrustData DMs via ID...")
            dms = get_decision_makers_by_id(cid) or []
            for p in dms:
                if not is_duplicate(p):
                    final_people.append(p)
        
        # 3. CRUSTDATA - Busca por cargos (LÓGICA ORIGINAL)
        if len(final_people) < 5:
            print("   ↳ CrustData C-Level/Mkt...")
            res = search_people_robust(cdom, ["marketing", "growth", "revenue", "sales", "ceo", "founder", "diretor"], limit=5) or []
            for p in res:
                if not is_duplicate(p):
                    final_people.append(p)

        if len(final_people) < 5:
            print("   ↳ CrustData Gerentes...")
            res = search_people_robust(cdom, ["manager", "gerente", "head"], limit=5) or []
            for p in res:
                if not is_duplicate(p):
                    final_people.append(p)

        if len(final_people) < 5:
            print("   ↳ CrustData Genérico...")
            res = search_people_robust(cdom, None, limit=5) or []
            for p in res:
                if not is_duplicate(p):
                    final_people.append(p)

        # 4. FALLBACK: Lusha (se CrustData não retornou suficiente)
        if len(final_people) < 5:
            print("   ↳ Fallback Lusha...")
            lusha_res = lusha_people_search(domain, ["CEO", "CMO", "Marketing", "Growth"], limit=5)
            for p in lusha_res:
                if not is_duplicate(p):
                    final_people.append(p)

        # 5. FALLBACK: Apollo (se ainda não tem suficiente)
        if len(final_people) < 5:
            print("   ↳ Fallback Apollo...")
            apollo_res = apollo_people_search(domain, ["CEO", "Founder", "Marketing", "Growth"], limit=5)
            for p in apollo_res:
                if not is_duplicate(p):
                    final_people.append(p)

        # 6. SEMPRE enriquece sócios via DataStone (email/celular) - para TODOS os portes
        # Sócios são contatos valiosos - donos da empresa
        socios_enriquecidos = []
        if socios and DATA_STONE_API_KEY:
            print("   ↳ Enriquecendo sócios via DataStone...")
            nome_fantasia = brasil_data.get("nome_fantasia", domain) if brasil_data else domain
            uf_empresa = brasil_data.get("uf") if brasil_data else None
            serper_calls = 0
            
            for socio in socios:
                nome = socio.get("nome")
                if not nome:
                    continue
                
                # Busca LinkedIn (limitado pelo MAX_SERPER_CALLS)
                linkedin_url = None
                if serper_calls < MAX_SERPER_CALLS:
                    serper_calls += 1
                    linkedin_url = search_linkedin_serper(nome, nome_fantasia)
                
                # ⭐ SEMPRE busca email/celular via DataStone (por NOME COMPLETO)
                print(f"   ↳ DataStone: Buscando {nome[:25]}...")
                datastone_data = fetch_datastone_person_by_name(nome, uf=uf_empresa)
                
                email_socio = None
                phone_socio = None
                if datastone_data:
                    email_socio = datastone_data.get("email")
                    phone_socio = datastone_data.get("phone")
                
                socio_enriquecido = {
                    "name": nome,
                    "full_name": nome,
                    "title": socio.get("qualificacao", "Sócio"),
                    "faixa_etaria": socio.get("faixa_etaria"),
                    "linkedin_url": linkedin_url,
                    "linkedin_profile_url": linkedin_url,
                    "email": email_socio,
                    "phone": phone_socio,
                    "source": "brasil_api" + (" + datastone" if datastone_data else "")
                }
                socios_enriquecidos.append(socio_enriquecido)
        
        # 7. Adiciona sócios enriquecidos na lista (se ainda tiver espaço e não for duplicado)
        for socio in socios_enriquecidos:
            if not is_duplicate(socio) and len(final_people) < 5:
                final_people.append(socio)

        final_people = final_people[:5]
        print(f"   ✅ Total Pessoas: {len(final_people)}")

        # 8. Calcula score final
        final_score = pre_score + (len(final_people) * 10)
        if final_score > 100:
            final_score = 100
        
        # 9. Atualiza Firestore
        doc_ref.update({
            "status": "ENRICHED",
            "people_data": final_people,
            "socios_enriquecidos": socios_enriquecidos,  # ⭐ Salva sócios enriquecidos separadamente
            "contacts_found": len(final_people),
            "final_score": final_score,
            "enriched_at": datetime.datetime.now()
        })
        
        # 10. Publica para HubSpot
        payload_closer = {
            "domain": domain,
            "company_name": comp_info.get("name", domain),
            "contacts": final_people,
            "socios": socios_enriquecidos,  # ⭐ Inclui sócios enriquecidos
            "final_score": final_score
        }
        publisher.publish(topic_path_closer, json.dumps(payload_closer).encode("utf-8"))
        
        # 11. Monta mensagem final CONCATENANDO o preview + contatos
        # ⭐ USA A MENSAGEM DE PREVIEW SALVA NO FIREBASE
        if preview_message:
            # Remove o score preliminar da mensagem de preview
            final_msg = preview_message.replace(f"📊 Score Preliminar: {pre_score}/100", "").strip()
            final_msg += f"\n\n✅ *Score Final: {final_score}/100* | Contatos: {len(final_people)}\n\n"
        else:
            # Fallback caso não tenha a mensagem salva (retrocompatibilidade)
            final_msg = f"🔎 *ANÁLISE DE LEAD (Completa)*\n"
            final_msg += f"🏢 *{clean_markdown(comp_info.get('name', domain))}* ({porte.upper()})\n"
            
            if comp_info.get('hq'):
                final_msg += f"📍 {clean_markdown(comp_info.get('hq'))} | 👥 {comp_info.get('employees', 'N/D')}\n"
            if comp_info.get('revenue'):
                final_msg += f"💰 Rev: {comp_info.get('revenue')}\n"
            
            # CNPJ
            if cnpj:
                cnpj_fmt = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
                final_msg += f"📋 CNPJ: {cnpj_fmt}\n"
                # Dados do Cartão CNPJ
                if brasil_data:
                    tel_cnpj = brasil_data.get("ddd_telefone_1")
                    email_cnpj = brasil_data.get("email")
                    if tel_cnpj:
                        final_msg += f"📞 Cartão CNPJ: {clean_markdown(tel_cnpj)}\n"
                    if email_cnpj:
                        final_msg += f"📧 Cartão CNPJ: {clean_markdown(email_cnpj)}\n"
            
            final_msg += "\n----------------\n"
            
            # Stack
            if tech_summary:
                if tech_summary.get('marketing'):
                    final_msg += f"📢 Mkt: {', '.join(tech_summary['marketing'][:4])}\n"
                if tech_summary.get('cms'):
                    final_msg += f"📝 CMS: {', '.join(tech_summary['cms'][:3])}\n"
                if tech_summary.get('analytics'):
                    final_msg += f"📈 Data: {', '.join(tech_summary['analytics'][:3])}\n"
            elif techs:
                final_msg += f"🛠 Stack: {', '.join(techs[:5])}\n"

            final_msg += "----------------\n"
            final_msg += f"✅ *Score: {final_score}/100* | Contatos: {len(final_people)}\n\n"
        
        # Lista de pessoas com formato rico
        if final_people:
            for p in final_people:
                final_msg += format_person_profile_full(p) + "\n\n"
        else:
            final_msg += "❌ Nenhuma pessoa encontrada nesta busca.\n"

        # EDITA a mensagem original (não apaga)
        edit_msg_final(chat_id, msg_id, final_msg)
        
        # 11. ENVIA NOVA MENSAGEM com botão de Copies (NÃO APAGA A ANTERIOR)
        copy_msg = f"✅ *{clean_markdown(comp_info.get('name', domain))}* processado!\n"
        copy_msg += f"📊 Score: {final_score} | 👥 {len(final_people)} contatos\n\n"
        copy_msg += "Deseja gerar copies personalizadas?"
        
        send_new_message_with_copies_button(chat_id, copy_msg, domain)

    except Exception as e:
        print(f"🔥 ERRO FATAL PARTE 2: {e}")
        traceback.print_exc()
        edit_msg_final(chat_id, msg_id, f"❌ Erro processando {domain} (Check Logs).")


# ==============================================================================
# 📨 CALLBACK PRINCIPAL
# ==============================================================================

def callback(message):
    """Callback principal do Pub/Sub"""
    try:
        data = json.loads(message.data.decode("utf-8"))
        
        if data.get("command") == "FETCH_PEOPLE":
            # Comando do botão "Enriquecer Pessoas"
            process_enrich_command_part2(data)
        else:
            # Novo lead vindo do Agente 2
            process_new_lead_part1(data)
        
        message.ack()
    except Exception as e:
        print(f"🔥 Erro Geral: {e}")
        traceback.print_exc()
        message.nack()


# ==============================================================================
# 🚀 MAIN
# ==============================================================================

if __name__ == "__main__":
    print(f"\n📡 Configuração:")
    print(f"   - CrustData: {'✅' if CRUST_API_KEY else '❌'}")
    print(f"   - Apollo: {'✅' if APOLLO_API_KEY else '❌'}")
    print(f"   - Lusha: {'✅' if LUSHA_API_KEY else '❌'}")
    print(f"   - Serper: {'✅' if SERPER_API_KEY else '❌'}")
    print(f"   - DataStone: {'✅' if DATA_STONE_API_KEY else '❌'}")
    
    if not CRUST_API_KEY:
        print("⚠️ AVISO: CRUST_API_KEY não configurada. Usando apenas Apollo/Lusha.")
    
    flow_control = pubsub_v1.types.FlowControl(max_messages=2)
    print(f"\n💎 Agente 3 (V4.8 - Sócios Universal) ouvindo...")
    
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control).result()
        except KeyboardInterrupt:
            print("\n👋 Agente 3 finalizado.")
