"""
DATABASE.PY - SalesMachine v4.0
COPIE ESTE ARQUIVO INTEIRO E SUBSTITUA O SEU database.py
"""
import datetime
import os
from google.cloud import firestore
from dotenv import load_dotenv
from datetime import timezone

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "databasecaracol")
COLLECTION_NAME = "leads_b2b"
COLLECTION_CNPJ_CACHE = "cnpj_cache"
COLLECTION_DEBUG = "debug_logs"
DIAS_PARA_REPROSPECTAR = 60
DIAS_CACHE_CNPJ = 180

print(f"🧠 Conectando ao Firestore: {PROJECT_ID}...")

db = None
try:
    db = firestore.Client(project=PROJECT_ID)
    print("✅ Banco Conectado!")
except Exception as e:
    print(f"❌ ERRO CRÍTICO NO BANCO: {e}")

def check_lead_exists(domain):
    if not db: return False
    try:
        doc_ref = db.collection(COLLECTION_NAME).document(domain)
        doc = doc_ref.get()
        if not doc.exists: return False
        data = doc.to_dict()
        last_date = data.get("created_at") or data.get("enriched_date")
        if not last_date: return False
        now = datetime.datetime.now(timezone.utc)
        if last_date.tzinfo is None:
            last_date = last_date.replace(tzinfo=timezone.utc)
        diferenca = now - last_date
        if diferenca.days > DIAS_PARA_REPROSPECTAR:
            print(f"   ♻️ Lead antigo ({diferenca.days} dias). Reciclando: {domain}")
            return False
        return True
    except Exception as e:
        print(f"⚠️ Erro ao checar banco: {e}")
        return True

def get_cnpj_cache(cnpj):
    if not db or not cnpj: return None
    try:
        cnpj_limpo = "".join(filter(str.isdigit, str(cnpj)))
        doc_ref = db.collection(COLLECTION_CNPJ_CACHE).document(cnpj_limpo)
        doc = doc_ref.get()
        if not doc.exists: return None
        data = doc.to_dict()
        cached_at = data.get("cached_at")
        if not cached_at: return None
        now = datetime.datetime.now(timezone.utc)
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        diferenca = now - cached_at
        if diferenca.days > DIAS_CACHE_CNPJ: return None
        print(f"   💾 Cache CNPJ válido: {cnpj_limpo}")
        return data.get("brasil_api_data")
    except Exception as e:
        print(f"⚠️ Erro cache CNPJ: {e}")
        return None

def save_cnpj_cache(cnpj, brasil_api_data):
    if not db or not cnpj: return
    try:
        cnpj_limpo = "".join(filter(str.isdigit, str(cnpj)))
        db.collection(COLLECTION_CNPJ_CACHE).document(cnpj_limpo).set({
            "cnpj": cnpj_limpo,
            "brasil_api_data": brasil_api_data,
            "cached_at": datetime.datetime.now(timezone.utc)
        })
        print(f"💾 [DB] Cache CNPJ salvo: {cnpj_limpo}")
    except Exception as e:
        print(f"⚠️ Erro salvar cache CNPJ: {e}")

def get_lead(domain):
    if not db: return None
    try:
        doc = db.collection(COLLECTION_NAME).document(domain).get()
        return doc.to_dict() if doc.exists else None
    except: return None

def save_new_lead(domain, origin_query):
    if not db: return
    try:
        db.collection(COLLECTION_NAME).document(domain).set({
            "domain": domain,
            "created_at": datetime.datetime.now(timezone.utc),
            "status": "NEW",
            "origin_query": origin_query
        }, merge=True)
        print(f"💾 [DB] Salvo: {domain}")
    except Exception as e:
        print(f"⚠️ Erro salvar lead: {e}")

def update_techs(domain, data_dict):
    if not db: return
    try:
        data_dict["tech_date"] = datetime.datetime.now(timezone.utc)
        data_dict["status"] = "TECH_OK"
        db.collection(COLLECTION_NAME).document(domain).set(data_dict, merge=True)
        print(f"💾 [DB] Techs: {domain}")
    except Exception as e:
        print(f"⚠️ Erro techs: {e}")

def update_enrichment(domain, data_dict):
    if not db: return
    try:
        data_dict["enriched_date"] = datetime.datetime.now(timezone.utc)
        data_dict["status"] = data_dict.get("status", "ENRICHED")
        db.collection(COLLECTION_NAME).document(domain).set(data_dict, merge=True)
        print(f"💾 [DB] Enrich: {domain}")
    except Exception as e:
        print(f"⚠️ Erro enrich: {e}")

def update_copies(domain, copies_data):
    if not db: return
    try:
        db.collection(COLLECTION_NAME).document(domain).set({
            "copies": copies_data,
            "copies_generated_at": datetime.datetime.now(timezone.utc),
            "status": "COPIES_READY"
        }, merge=True)
        print(f"💾 [DB] Copies: {domain}")
    except Exception as e:
        print(f"⚠️ Erro copies: {e}")

def save_debug_log(agent_name, direction, payload, domain=None):
    if not db: return
    try:
        doc_id = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{agent_name}_{direction}"
        db.collection(COLLECTION_DEBUG).document(doc_id).set({
            "timestamp": datetime.datetime.now(timezone.utc),
            "agent": agent_name,
            "direction": direction,
            "domain": domain or (payload.get("domain", "?") if isinstance(payload, dict) else "?"),
            "payload_preview": str(payload)[:2000]
        })
    except: pass
