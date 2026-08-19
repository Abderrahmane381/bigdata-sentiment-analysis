from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pymongo import MongoClient
import re, os

# ── Config ──────────────────────────────────────────────
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
DATA_PATH = "/data/Reviews.csv"
MODEL_PATH = "/data/sentiment_model"
SENTIMENT_MAP = {0.0: "negatif", 1.0: "neutre", 2.0: "positif"}

# ── SparkSession ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("TrainSentimentModel") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Nettoyage texte ──────────────────────────────────────
def clean_text(text):
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    return text

clean_udf = udf(clean_text, StringType())

# ── 1. Charger données ───────────────────────────────────
print("📦 Chargement des données...")
df = spark.read.csv(DATA_PATH, header=True, inferSchema=False)
df = df.filter(col("Score").isin("1", "2", "3", "4", "5"))
df = df.withColumn("Score", col("Score").cast(IntegerType()))
df = df.withColumn("label",
    when(col("Score") > 3, 2.0)
    .when(col("Score") == 3, 1.0)
    .otherwise(0.0)
)
df = df.withColumn("clean_text", clean_udf(col("Text")))
df = df.select("clean_text", "label", "ProductId", "Time").na.drop()
print(f"✅ Total : {df.count()} reviews")

# ── 2. Split 80/10/10 ────────────────────────────────────
train, validation, test = df.randomSplit([0.8, 0.1, 0.1], seed=42)
print(f"✅ Train: {train.count()} | Val: {validation.count()} | Test: {test.count()}")

# ── 3. Entraîner le modèle ───────────────────────────────
print("🔧 Entraînement...")
tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=10)
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
model = pipeline.fit(train)
print("✅ Modèle entraîné")

# ── 4. Évaluation ────────────────────────────────────────
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
val_acc = evaluator.evaluate(model.transform(validation))
test_acc = evaluator.evaluate(model.transform(test))
print(f"✅ Val Accuracy: {val_acc:.4f} | Test Accuracy: {test_acc:.4f}")

# ── 5. Sauvegarder modèle ────────────────────────────────
model.write().overwrite().save(MODEL_PATH)
print("✅ Modèle sauvegardé")

# ── 6. Stocker dans MongoDB ──────────────────────────────
client = MongoClient(MONGO_URI)
db = client["amazon_sentiment"]
db.model_metrics.drop()
db.test_predictions.drop()

# Métriques
db.model_metrics.insert_one({
    "model": "LogisticRegression",
    "val_accuracy": round(val_acc, 4),
    "test_accuracy": round(test_acc, 4),
    "train_size": train.count(),
    "val_size": validation.count(),
    "test_size": test.count()
})
print("✅ Métriques stockées")

# Prédictions test pour dashboard off-line
test_preds = model.transform(test).select(
    "clean_text", "label", "prediction", "ProductId", "Time"
).limit(5000).collect()

docs = []
for i, row in enumerate(test_preds):
    docs.append({
        "order": i,
        "text": row["clean_text"][:100],
        "true_label": SENTIMENT_MAP.get(row["label"], "unknown"),
        "predicted_label": SENTIMENT_MAP.get(row["prediction"], "unknown"),
        "correct": row["label"] == row["prediction"],
        "ProductId": row["ProductId"],
        "timestamp": int(row["Time"]) if row["Time"] else 0
    })

db.test_predictions.insert_many(docs)
print(f"✅ {len(docs)} prédictions test stockées dans MongoDB")

# Sauvegarder test set pour le producer
test_df = test.select("clean_text", "label", "ProductId", "Time").limit(5000)
test_rows = test_df.collect()

test_docs = []
for i, row in enumerate(test_rows):
    test_docs.append({
        "order": i,
        "text": row["clean_text"],
        "label": row["label"],
        "ProductId": row["ProductId"],
        "Time": row["Time"]
    })

db.test_set.drop()
db.test_set.insert_many(test_docs)
print(f"✅ {len(test_docs)} reviews test sauvegardées pour le producer")

client.close()
spark.stop()
print("🎉 Entraînement terminé !")
