import os
from google.cloud import firestore
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
# As coleções que identificamos no seu sistema
COLLECTIONS_TO_CLEAN = ["leads_b2b", "cnpj_cache", "debug_logs"]

def delete_collection(coll_ref, batch_size):
    """Apaga os documentos de uma coleção em blocos (batches)"""
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        doc.reference.delete()
        deleted += 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)
    return deleted

def main():
    if not PROJECT_ID:
        print("❌ Erro: GCP_PROJECT_ID não encontrado no .env.")
        return

    print(f"🔥 Preparando para limpar o banco: {PROJECT_ID}")
    confirm = input("⚠️ Isso apagará TODOS os dados das coleções de teste. Tem certeza? (s/n): ")
    
    if confirm.lower() != 's':
        print("Ufa! Operação cancelada.")
        return

    db = firestore.Client(project=PROJECT_ID)

    for coll_name in COLLECTIONS_TO_CLEAN:
        print(f"🧹 Limpando coleção: {coll_name}...")
        coll_ref = db.collection(coll_name)
        total_deleted = delete_collection(coll_ref, 100)
        print(f"✅ Concluído: {total_deleted} documentos removidos de {coll_name}.")

    print("\n✨ O banco de dados está limpo e pronto para novos testes!")

if __name__ == "__main__":
    main()
