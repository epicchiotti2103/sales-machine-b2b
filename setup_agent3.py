import os
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists
from dotenv import load_dotenv

load_dotenv()
project_id = os.getenv("GCP_PROJECT_ID")
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# 1. Tópico de Saída (Onde o Agente 3 vai jogar o resultado final para o HubSpot)
topic_hubspot = publisher.topic_path(project_id, "topic-closer-hubspot")
try:
    publisher.create_topic(request={"name": topic_hubspot})
    print("✅ Tópico 'topic-closer-hubspot' criado.")
except AlreadyExists: print("⚠️ Tópico 'topic-closer-hubspot' já existe.")

# 2. Assinatura de Entrada (Onde o Agente 3 vai LER o que o Agente 2 produziu)
# Ele conecta na fila 'topic-enricher' que o Agente 2 já está enchendo
topic_source = publisher.topic_path(project_id, "topic-enricher")
sub_path = subscriber.subscription_path(project_id, "sub-enricher-worker")

try:
    subscriber.create_subscription(request={"name": sub_path, "topic": topic_source})
    print("✅ Assinatura 'sub-enricher-worker' criada.")
except AlreadyExists: print("⚠️ Assinatura já existe.")
