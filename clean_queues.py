import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()
project_id = os.getenv("GCP_PROJECT_ID")
subscriber = pubsub_v1.SubscriberClient()

# Lista de todas as assinaturas do nosso projeto
subscriptions = [
    "sub-telegram-input",   # Fila do Agente 1
    "sub-tech-checker",     # Fila do Agente 2
    "sub-enricher-worker"   # Fila do Agente 3
]

print(f"🧹 INICIANDO FAXINA NO PROJETO: {project_id}...\n")

for sub_name in subscriptions:
    subscription_path = subscriber.subscription_path(project_id, sub_name)
    print(f"🗑️ Verificando fila: {sub_name}...")
    
    try:
        # Tenta puxar até 100 mensagens de uma vez
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 100}
        )
        
        if not response.received_messages:
            print("   ✅ Fila já estava vazia.")
            continue

        ack_ids = [msg.ack_id for msg in response.received_messages]
        
        # Confirma o recebimento (isso deleta a mensagem da fila)
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
        
        print(f"   🔥 {len(ack_ids)} mensagens antigas deletadas!")
        
    except Exception as e:
        print(f"   ⚠️ Fila vazia ou inexistente (pode ignorar).")

print("\n✨ Tudo limpo!")
