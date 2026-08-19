from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml import PipelineModel
from pymongo import MongoClient
import re, os, time
import requests
# ── Config ──────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MODEL_PATH = "/data/sentiment_model"
SENTIMENT_MAP = {0.0: "negatif", 1.0: "neutre", 2.0: "positif"}

# ── SparkSession ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SentimentStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Attendre que le modèle soit prêt ────────────────────
print("⏳ Attente du modèle entraîné...")
while not os.path.exists(MODEL_PATH):
    time.sleep(10)
    print("⏳ Modèle pas encore prêt, attente...")

print("✅ Modèle trouvé, chargement...")
model = PipelineModel.load(MODEL_PATH)
print("✅ Modèle chargé !")

# ── Nettoyage texte ──────────────────────────────────────
def clean_text(text):
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    return text

clean_udf = udf(clean_text, StringType())

# ── Schema Kafka ─────────────────────────────────────────
schema = StructType([
    StructField("id", StringType()),
    StructField("score", StringType()),
    StructField("summary", StringType()),
    StructField("text", StringType()),
    StructField("order", StringType())
])

# ── Lire depuis Kafka ────────────────────────────────────
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", "amazon-reviews") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ── Traitement par batch ─────────────────────────────────
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df = batch_df.withColumn("clean_text", clean_udf(col("text")))
    batch_df = batch_df.withColumn("label",
        when(col("score") > "3", 2.0).otherwise(0.0))

    predictions = model.transform(
        batch_df.select("clean_text", "label", "text", "score", "order")
    )

    client = MongoClient(MONGO_URI)
    collection = client["amazon_sentiment"]["streaming_predictions"]

    rows = predictions.collect()
    for row in rows:
        doc = {
            "order": int(row["order"]) if row["order"] else 0,
            "text": row["text"][:150] if row["text"] else "",
            "score": row["score"],
            "predicted_sentiment": SENTIMENT_MAP.get(row["prediction"], "unknown"),
            "sentiment_value": int(row["prediction"])
        }
        # Stocker dans MongoDB
        collection.insert_one(doc)
        
        # Envoyer au dashboard via FastAPI WebSocket
        try:
            requests.post("http://api:8000/predict", json=doc, timeout=1)
        except:
            pass

    print(f"⚡ Batch {batch_id}: {len(rows)} reviews → MongoDB + Dashboard ✅")
    client.close()
# ── Lancer streaming ─────────────────────────────────────
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime='5 seconds') \
    .start()

print("🚀 Spark Streaming actif !")
query.awaitTermination()
