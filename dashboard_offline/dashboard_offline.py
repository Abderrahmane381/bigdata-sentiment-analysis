import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os

st.set_page_config(page_title="Off-line Dashboard", page_icon="📈", layout="wide")

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongodb:27017/')
client = MongoClient(MONGO_URI)
db = client["amazon_sentiment"]

st.title("📈 Dashboard Off-line — Données Test")

if st.button("🔄 Actualiser"):
    st.rerun()

# Métriques modèle
metrics = db.model_metrics.find_one()
if metrics:
    col1, col2, col3 = st.columns(3)
    col1.metric("🎯 Val Accuracy", f"{metrics['val_accuracy']*100:.2f}%")
    col2.metric("🎯 Test Accuracy", f"{metrics['test_accuracy']*100:.2f}%")
    col3.metric("📊 Modèle", metrics['model'])

# Distribution
total = db.test_predictions.count_documents({})
st.write(f"Total prédictions test : {total}")

if total > 0:
    pos = db.test_predictions.count_documents({"predicted_label": "positif"})
    neu = db.test_predictions.count_documents({"predicted_label": "neutre"})
    neg = db.test_predictions.count_documents({"predicted_label": "negatif"})

    st.subheader("Distribution des sentiments")
    df = pd.DataFrame({
        "Sentiment": ["Positif", "Neutre", "Négatif"],
        "Count": [pos, neu, neg]
    })
    st.bar_chart(df.set_index("Sentiment"))

    correct = db.test_predictions.count_documents({"correct": True})
    st.metric("✅ Accuracy", f"{correct/total*100:.2f}%")

    st.subheader("Top 10 produits")
    pipeline = [
        {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top = list(db.test_predictions.aggregate(pipeline))
    if top:
        df2 = pd.DataFrame(top)
        df2.columns = ["ProductId", "Count"]
        st.bar_chart(df2.set_index("ProductId"))
else:
    st.warning("Pas de données test disponibles")
