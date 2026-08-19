from kafka import KafkaProducer
from pymongo import MongoClient
import json, os, time

KAFKA_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongodb:27017/')

# ── Attendre Kafka ───────────────────────────────────────
print("⏳ Attente de Kafka...")
time.sleep(30)

# ── Attendre que le test set soit prêt dans MongoDB ─────
print("⏳ Attente du test set dans MongoDB...")
client = MongoClient(MONGO_URI)
db = client["amazon_sentiment"]

while db.test_set.count_documents({}) == 0:
    print("⏳ Test set pas encore prêt...")
    time.sleep(10)

print("✅ Test set prêt !")

# ── Producer Kafka ───────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ── Envoyer les reviews test une par une ────────────────
reviews = list(db.test_set.find({}).sort("order", 1))
print(f"📦 {len(reviews)} reviews à envoyer...")

for review in reviews:
    message = {
        "id": str(review.get("order", 0)),
        "order": str(review.get("order", 0)),
        "score": str(int(review.get("label", 0))),
        "summary": "",
        "text": review.get("text", "")
    }
    producer.send('amazon-reviews', value=message)
    print(f"⚡ Review {review['order']} envoyée")
    time.sleep(0.5)  # 1 review toutes les 0.5 secondes

producer.flush()
print("✅ Toutes les reviews envoyées !")
client.close()
