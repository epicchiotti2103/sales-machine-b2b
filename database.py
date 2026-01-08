import datetime
from google.cloud import firestore

# --- CONFIGURAÇÃO ---
# ID do projeto forçado conforme seu pedido
PROJECT_ID = "databasecaracol"
COLLECTION_NAME = "leads_b2b"

print(f"🧠 Conectando ao Firestore: {PROJECT_ID}...")

try:
    # Tenta conectar. Se der erro de credencial, ele avisa.
    db = firestore.Client(project=PROJECT_ID)
    print("✅ Banco Conectado!")
except Exception as e:
    print(f"❌ ERRO CRÍTICO NO BANCO: {e}")
    db = None

def check_lead_exists(domain):
    """Retorna True se o lead já estiver cadastrado."""
    if not db: return False
    try:
        doc = db.collection(COLLECTION_NAME).document(domain).get()
        return doc.exists
    except Exception as e:
        print(f"⚠️ Erro ao checar banco: {e}")
        return False

def save_new_lead(domain, origin_query):
    """Cria o lead com status NEW."""
    if not db: return
    try:
        doc_ref = db.collection(COLLECTION_NAME).document(domain)
        # Só salva se não existir (garantia dupla)
        if not doc_ref.get().exists:
            doc_ref.set({
                "domain": domain,
                "created_at": datetime.datetime.now(),
                "status": "NEW",
                "origin_query": origin_query,
                "history": [f"Criado via busca: {origin_query}"]
            })
            print(f"💾 [DB] Salvo: {domain}")
    except Exception as e:
        print(f"⚠️ Erro ao salvar lead: {e}")

def update_techs(domain, techs):
    """Agente 2 chama isso."""
    if not db: return
    try:
        db.collection(COLLECTION_NAME).document(domain).update({
            "techs": techs,
            "status": "TECH_OK",
            "tech_date": datetime.datetime.now()
        })
        print(f"💾 [DB] Techs atualizadas: {domain}")
    except: pass

def update_enrichment(domain, data_dict):
    """Agente 3 chama isso (Salva Score, Emails, Fontes)."""
    if not db: return
    try:
        # Adiciona data de atualização
        data_dict["enriched_date"] = datetime.datetime.now()
        data_dict["status"] = "ENRICHED"
        
        db.collection(COLLECTION_NAME).document(domain).update(data_dict)
        print(f"💾 [DB] Enriquecimento salvo: {domain}")
    except Exception as e:
        print(f"⚠️ Erro update enrich: {e}")
