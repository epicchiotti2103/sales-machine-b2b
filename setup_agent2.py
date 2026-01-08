import os
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists
from dotenv import load_dotenv

load_dotenv()
project_id = os.getenv("GCP_PROJECT_ID")
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# 1. Cria o Tópico de Saída (Enricher)
topic_enricher = publisher.topic_path(project_id, "topic-enricher")
try:
    publisher.create_topic(request={"name": topic_enricher})
    print("✅ Tópico 'topic-enricher' criado.")
except AlreadyExists: print("⚠️ Tópico 'topic-enricher' já existe.")

# 2. Cria a Assinatura de Entrada (Lê do Tech Filter)
topic_source = publisher.topic_path(project_id, "topic-tech-filter")
sub_path = subscriber.subscription_path(project_id, "sub-tech-checker")

try:
    subscriber.create_subscription(request={"name": sub_path, "topic": topic_source})
    print("✅ Assinatura 'sub-tech-checker' criada.")
except AlreadyExists: print("⚠️ Assinatura já existe.")
