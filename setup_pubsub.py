import os
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists
from dotenv import load_dotenv

# Carrega suas senhas e configurações
load_dotenv()

project_id = os.getenv("GCP_PROJECT_ID")
topic_agent_1 = os.getenv("TOPIC_AGENT_1")      # topic-discovery-input
topic_tech = os.getenv("TOPIC_TECH_FILTER")     # topic-tech-filter
sub_telegram = os.getenv("SUBSCRIPTION_TELEGRAM") # sub-telegram-input

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

def create_topic(name):
    topic_path = publisher.topic_path(project_id, name)
    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"✅ Tópico criado: {name}")
    except AlreadyExists:
        print(f"⚠️ Tópico já existe: {name} (Tudo certo!)")
    except Exception as e:
        print(f"❌ Erro ao criar {name}: {e}")

def create_subscription(sub_name, topic_name):
    topic_path = publisher.topic_path(project_id, topic_name)
    sub_path = subscriber.subscription_path(project_id, sub_name)
    try:
        subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        print(f"✅ Assinatura criada: {sub_name} (conectada a {topic_name})")
    except AlreadyExists:
        print(f"⚠️ Assinatura já existe: {sub_name} (Tudo certo!)")
    except Exception as e:
        print(f"❌ Erro ao criar {sub_name}: {e}")

if __name__ == "__main__":
    print(f"👷 Iniciando obras no projeto: {project_id}...")
    
    # 1. Cria os Tópicos (Os trilhos)
    create_topic(topic_agent_1)
    create_topic(topic_tech)
    
    # 2. Cria a Assinatura (O cozinheiro olhando o trilho)
    create_subscription(sub_telegram, topic_agent_1)
    
    print("🎉 Infraestrutura Pub/Sub pronta!")
