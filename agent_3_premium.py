import os
import json
import requests
import threading
import datetime
import traceback
import re
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

load_dotenv()
print("\n💎 --- AGENTE 3: ENRICHER (V5.9 - Stability Patch) ---")

# --- Configurações ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CRUST_API_KEY = os.getenv("CRUST_API_KEY")

SUBSCRIPTION_INPUT = "sub-enricher-worker"
TOPIC_CLOSER = "topic-closer-hubspot"
BASE_URL = "https://api.crustdata.com"

# --- GCP Setup ---
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path_closer = publisher.topic_path(PROJECT_ID, TOPIC_CLOSER)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)
db_firestore = firestore.Client(project=PROJECT_ID)

# ==============================================================================
# 🛠️ UTILITÁRIOS
# ==============================================================================

def clean_markdown(text):
    """Remove caracteres que quebram o Markdown"""
    if not text: return ""
    return text.replace("_", " ").replace("*", " ").replace("`", "'").replace("[", "(").replace("]", ")")

def parse_date_ym(date_str):
    if not date_str: return None
    try:
        return datetime.datetime.fromisoformat(date_str.replace("Z", "")).strftime("%Y-%m")
    except: return date_str

def format_person_profile_full(person):
    """Gera o card rico do funcionário"""
    name = clean_markdown(person.get("full_name") or person.get("name"))
    linkedin = person.get("linkedin_profile_url") or person.get("linkedin_url")
    location = clean_markdown(person.get("city") or person.get("location_city") or person.get("location"))

    employers = person.get("employer") or []
    if not isinstance(employers, list): employers = []

    current = None
    if employers:
        for e in employers:
            if e.get("is_default"): current = e; break
        if current is None and employers:
            current = sorted(employers, key=lambda e: e.get("start_date") or "", reverse=True)[0]

    lines = []
    
    # 1. Header
    if current:
        title = clean_markdown(current.get("title"))
        c_name = clean_markdown(current.get("company_name"))
        line1 = f"👤 *{name}* — {title}"
        if location: line1 += f" ({location})"
    else:
        line1 = f"👤 *{name or 'Contato'}*"
    lines.append(line1)

    # 2. Descrição
    if current:
        start = parse_date_ym(current.get("start_date"))
        base = f"Start: {start}" if start else "Atual"
        desc = current.get("description") or person.get("headline") or person.get("summary")
        if desc:
            clean_desc = clean_markdown(desc.strip().replace("\n", " "))
            if len(clean_desc) > 120: clean_desc = clean_desc[:117] + "..."
            lines.append(f"   _{base} | {clean_desc}_")
        else:
            lines.append(f"   _{base}_")

    # 3. Histórico
    if employers:
        sorted_emp = sorted(employers, key=lambda e: e.get("start_date") or "", reverse=True)
        hist_lines = []
        count = 0
        for e in sorted_emp:
            if current and e is current: continue
            s = parse_date_ym(e.get("start_date"))
            end = parse_date_ym(e.get("end_date")) or "atual"
            tit = clean_markdown(e.get("title"))
            cmp = clean_markdown(e.get("company_name"))
            hist_lines.append(f"   • {s if s else '?'} - {end}: {tit} @ {cmp}")
            count += 1
            if count >= 3: break
        
        if hist_lines:
            lines.append("   📜 *Histórico:*")
            lines.extend(hist_lines)

    if linkedin:
        lines.append(f"   🔗 [LinkedIn]({linkedin})")

    return "\n".join(lines)

# ==============================================================================
# 📡 TELEGRAM
# ==============================================================================

def send_telegram_preview(chat_id, text, domain):
    keyboard = {
        "inline_keyboard": [[
            {"text": "👥 Enriquecer Funcionários", "callback_data": f"ENRICH:{domain}"},
            {"text": "🗑 Descartar", "callback_data": f"DISCARD:{domain}"}
        ]]
    }
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard})
        if r.status_code != 200:
            requests.post(url, json={"chat_id": chat_id, "text": text, "reply_markup": keyboard})
    except: pass

def edit_msg_final(chat_id, msg_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    try:
        payload = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True}
        r = requests.post(url, json=payload)
        if r.status_code != 200:
            payload["parse_mode"] = None
            requests.post(url, json=payload)
    except Exception as e:
        print(f"⚠️ Falha envio Telegram: {e}")

# ==============================================================================
# 📡 CRUST DATA (Blindado contra NoneType)
# ==============================================================================

def enrich_company_basic(domain):
    if not CRUST_API_KEY: return None
    try:
        url = f"{BASE_URL}/screener/company"
        resp = requests.get(url, headers={"Authorization": f"Token {CRUST_API_KEY}"}, params={"company_domain": domain}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data[0] if isinstance(data, list) and len(data) > 0 else None
    except: pass
    return None

def get_decision_makers_by_id(company_id):
    """Retorna lista vazia se falhar, nunca None"""
    if not company_id or not CRUST_API_KEY: return []
    try:
        url = f"{BASE_URL}/screener/company"
        params = {"company_id": str(company_id), "fields": "decision_makers"}
        resp = requests.get(url, headers={"Authorization": f"Token {CRUST_API_KEY}"}, params=params, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data: 
                return data[0].get("decision_makers") or [] # Garante lista
            elif isinstance(data, dict): 
                return data.get("decision_makers") or [] # Garante lista
    except Exception as e:
        print(f"⚠️ Erro ID Search: {e}")
    return [] # Sempre retorna lista

def search_people_robust(company_key, titles_list=None, limit=5):
    """Retorna lista vazia se falhar, nunca None"""
    if not company_key or not CRUST_API_KEY: return []
    url = f"{BASE_URL}/screener/person/search/"
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
        print(f"⚠️ Erro Search: {e}")
    return [] # Sempre retorna lista

# ==============================================================================
# 🔄 PARTE 1
# ==============================================================================

def process_new_lead_part1(data):
    domain = data.get("domain")
    chat_id = data.get("chat_id")
    techs = data.get("techs", [])
    tech_summary = data.get("tech_summary", {})
    tech_score = data.get("tech_score", 0)
    
    print(f"\n🔵 [Parte 1] Analisando Empresa: {domain}")
    
    comp_info = enrich_company_basic(domain)
    
    comp_payload = {}
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
            "description": comp_info.get("linkedin_company_description", "")
        }
    else:
        comp_payload = {"name": domain, "revenue": "N/D"}

    msg = f"🔎 *ANÁLISE DE LEAD (Revisão)*\n"
    msg += f"🏢 *{clean_markdown(comp_payload.get('name'))}*\n"
    if comp_info:
        msg += f"📍 {clean_markdown(comp_payload.get('hq', 'N/D'))} | 👥 {comp_payload.get('employees', 'N/D')}\n"
        msg += f"💰 Rev: {comp_payload.get('revenue')}\n"
        if comp_payload.get('description'):
            msg += f"📝 _{clean_markdown(comp_payload['description'][:150])}..._\n"
    
    msg += "----------------\n"
    enrich_points = 20 if comp_info else 0
    pre_score = int(tech_score * 0.5) + enrich_points
    msg += f"📊 Score Preliminar: {pre_score}/100\n"

    if tech_summary:
        if tech_summary.get('marketing'): msg += f"📢 Mkt: {', '.join(tech_summary['marketing'])}\n"
        if tech_summary.get('cms'): msg += f"📝 CMS: {', '.join(tech_summary['cms'])}\n"
        if tech_summary.get('analytics'): msg += f"📈 Data: {', '.join(tech_summary['analytics'])}\n"
        others = [t for t in techs if t not in set(tech_summary.get('marketing', []) + tech_summary.get('cms', []) + tech_summary.get('analytics', []))]
        if others: msg += f"⚙️ Infra: {', '.join(others[:5])}\n"
    elif techs:
        msg += f"🛠 Stack: {', '.join(techs[:6])}\n"
        
    msg += "----------------"
    
    db_data = {
        "tech_score": tech_score,
        "preliminary_score": pre_score,
        "crust_company": comp_payload,
        "tech_data": techs,
        "tech_summary": tech_summary,
        "status": "WAITING_DECISION",
        "contacts_found": 0,
        "last_update": datetime.datetime.now()
    }
    database.update_enrichment(domain, db_data)
    send_telegram_preview(chat_id, msg, domain)

# ==============================================================================
# 🔄 PARTE 2 (ANTI-BUG)
# ==============================================================================

def process_enrich_command_part2(data):
    domain = data.get("domain")
    chat_id = data.get("chat_id")
    msg_id = data.get("message_id")

    print(f"\n🟢 [Parte 2] Enriquecendo Pessoas: {domain}")

    try:
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

        cid = comp_info.get("id")
        cdom = comp_info.get("domain") or domain

        final_people = []
        seen_ids = set()

        # 1. DMs via ID (Com blindagem "or []")
        if cid:
            print("   ↳ DMs via ID...")
            dms = get_decision_makers_by_id(cid) or [] # <--- BLINDAGEM AQUI
            for p in dms:
                pid = p.get("person_id") or p.get("linkedin_profile_url")
                if pid and pid not in seen_ids: seen_ids.add(pid); final_people.append(p)
        
        # 2. Fallbacks via Domínio
        if len(final_people) < 5:
            print("   ↳ C-Level/Mkt...")
            res = search_people_robust(cdom, ["marketing", "growth", "revenue", "sales", "ceo", "founder", "diretor"], limit=5) or []
            for p in res:
                pid = p.get("person_id") or p.get("linkedin_profile_url")
                if pid and pid not in seen_ids: seen_ids.add(pid); final_people.append(p)

        if len(final_people) < 5:
            print("   ↳ Gerentes...")
            res = search_people_robust(cdom, ["manager", "gerente", "head"], limit=5) or []
            for p in res:
                pid = p.get("person_id") or p.get("linkedin_profile_url")
                if pid and pid not in seen_ids: seen_ids.add(pid); final_people.append(p)

        if len(final_people) < 5:
            print("   ↳ Genérico...")
            res = search_people_robust(cdom, None, limit=5) or []
            for p in res:
                pid = p.get("person_id") or p.get("linkedin_profile_url")
                if pid and pid not in seen_ids: seen_ids.add(pid); final_people.append(p)

        final_people = final_people[:5]
        print(f"   ✅ Pessoas: {len(final_people)}")

        final_score = pre_score + (len(final_people) * 10)
        if final_score > 100: final_score = 100
        
        doc_ref.update({
            "status": "SENT_TO_HUBSPOT",
            "people_data": final_people,
            "contacts_found": len(final_people),
            "final_score": final_score,
            "enriched_at": datetime.datetime.now()
        })
        
        payload_closer = {
            "domain": domain,
            "company_name": comp_info.get("name", domain),
            "contacts": final_people,
            "final_score": final_score
        }
        publisher.publish(topic_path_closer, json.dumps(payload_closer).encode("utf-8"))
        
        # Reconstrói MSG
        final_msg = f"🔎 *ANÁLISE DE LEAD (Final)*\n"
        final_msg += f"🏢 *{clean_markdown(comp_info.get('name', domain))}*\n"
        final_msg += f"📍 {clean_markdown(comp_info.get('hq', 'N/D'))} | 👥 {comp_info.get('employees', 'N/D')}\n"
        
        if comp_info.get('revenue'): final_msg += f"💰 Rev: {comp_info.get('revenue')}\n"
        
        final_msg += "----------------\n"
        
        if tech_summary:
            if tech_summary.get('marketing'): final_msg += f"📢 Mkt: {', '.join(tech_summary['marketing'])}\n"
            if tech_summary.get('cms'): final_msg += f"📝 CMS: {', '.join(tech_summary['cms'])}\n"
            if tech_summary.get('analytics'): final_msg += f"📈 Data: {', '.join(tech_summary['analytics'])}\n"
            others = [t for t in techs if t not in set(tech_summary.get('marketing', []) + tech_summary.get('cms', []) + tech_summary.get('analytics', []))]
            if others: final_msg += f"⚙️ Infra: {', '.join(others[:4])}\n"
        elif techs:
            final_msg += f"🛠 Stack: {', '.join(techs[:5])}\n"

        final_msg += "----------------\n"
        final_msg += f"✅ *ENVIADO HUBSPOT* (Score: {final_score})\n\n"
        
        if final_people:
            for p in final_people:
                final_msg += format_person_profile_full(p) + "\n\n"
        else:
            final_msg += "❌ Nenhuma pessoa encontrada nesta busca.\n"

        edit_msg_final(chat_id, msg_id, final_msg)

    except Exception as e:
        print(f"🔥 ERRO FATAL PARTE 2: {e}")
        traceback.print_exc()
        edit_msg_final(chat_id, msg_id, f"❌ Erro processando {domain} (Check Logs).")

# ==============================================================================
# 📨 CALLBACK
# ==============================================================================

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        if data.get("command") == "FETCH_PEOPLE":
            process_enrich_command_part2(data)
        else:
            process_new_lead_part1(data)
        message.ack()
    except Exception as e:
        print(f"🔥 Erro Geral: {e}")
        message.nack()

if __name__ == "__main__":
    if not CRUST_API_KEY:
        print("❌ ERRO: CRUST_API_KEY não encontrada.")
    else:
        flow_control = pubsub_v1.types.FlowControl(max_messages=2)
        print(f"💎 Agente 3 (V5.9 - Stability Patch) ouvindo...")
        with subscriber:
            try:
                subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control).result()
            except KeyboardInterrupt: pass
