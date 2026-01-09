import datetime
import os
from google.cloud import firestore
from dotenv import load_dotenv
from datetime import timezone

load_dotenv()

# --- CONFIGURAÇÃO ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "databasecaracol")
COLLECTION_NAME = "leads_b2b"

# --- MUDANÇA AQUI: Agora são 60 dias de intervalo ---
DIAS_PARA_REPROSPECTAR = 60 

print(f"🧠 Conectando ao Firestore: {PROJECT_ID}...")

db = None
try:
    db = firestore.Client(project=PROJECT_ID)
    print("✅ Banco Conectado!")
except Exception as e:
    print(f"❌ ERRO CRÍTICO NO BANCO: {e}")

def check_lead_exists(domain):
    """
    Retorna True se o lead existe E foi processado nos últimos 60 dias.
    Se for antigo (mais de 60 dias), retorna False (libera para prospectar de novo).
    """
    if not db: return False
    try:
        doc_ref = db.collection(COLLECTION_NAME).document(domain)
        doc = doc_ref.get()
        
        if not doc.exists:
            return False # É novo de verdade, pode processar
            
        data = doc.to_dict()
        # Pega a data de criação ou última atualização
        last_date = data.get("created_at") or data.get("enriched_date")
        
        if not last_date:
            return False # Se não tem data, processa de novo por garantia
            
        # Verifica se passou da validade (Aware Timezone para evitar erro)
        now = datetime.datetime.now(timezone.utc)
        
        # Se a data do banco não tiver timezone, forçamos UTC
        if last_date.tzinfo is None:
            last_date = last_date.replace(tzinfo=timezone.utc)
            
        diferenca = now - last_date
        
        if diferenca.days > DIAS_PARA_REPROSPECTAR:
            print(f"   ♻️ Lead antigo ({diferenca.days} dias). Reciclando: {domain}")
            return False # Libera para processar de novo!
        
        return True # Está na quarentena, ignorar.
        
    except Exception as e:
        print(f"⚠️ Erro ao checar banco: {e}")
        # Na dúvida se o banco falhar, melhor não processar para evitar custo
        return True 

def save_new_lead(domain, origin_query):
    """Cria ou Atualiza o lead com nova data."""
    if not db: return
    try:
        doc_ref = db.collection(COLLECTION_NAME).document(domain)
        
        # .set com merge=True atualiza a data se já existir, ou cria se for novo
        doc_ref.set({
            "domain": domain,
            "created_at": datetime.datetime.now(timezone.utc), # Reseta o relógio
            "status": "NEW",
            "origin_query": origin_query,
            "last_touch": "Reciclado" # Marcação para saber que voltou
        }, merge=True)
        
        print(f"💾 [DB] Salvo/Atualizado: {domain}")
            
    except Exception as e:
        print(f"⚠️ Erro ao salvar lead: {e}")

def update_techs(domain, techs):
    """Chamado pelo Agente 2"""
    if not db: return
    try:
        db.collection(COLLECTION_NAME).document(domain).set({
            "techs": techs,
            "status": "TECH_OK",
            "tech_date": datetime.datetime.now(timezone.utc)
        }, merge=True)
        print(f"💾 [DB] Techs atualizadas: {domain}")
    except Exception as e:
        print(f"⚠️ Erro update techs: {e}")

def update_enrichment(domain, data_dict):
    """Chamado pelo Agente 3"""
    if not db: return
    try:
        data_dict["enriched_date"] = datetime.datetime.now(timezone.utc)
        data_dict["status"] = "ENRICHED"
        db.collection(COLLECTION_NAME).document(domain).set(data_dict, merge=True)
        print(f"💾 [DB] Enriquecimento salvo: {domain}")
    except Exception as e:
        print(f"⚠️ Erro update enrich: {e}")
