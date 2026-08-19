import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time, os

st.set_page_config(
    page_title="Amazon Sentiment Dashboard",
    page_icon="📊",
    layout="wide"
)

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongodb:27017/')

def get_db():
    client = MongoClient(MONGO_URI)
    return client["amazon_sentiment"]

st.title("📊 Amazon Reviews — Sentiment Analysis")

db = get_db()

# Métriques
total = db.streaming_predictions.count_documents({})
positif = db.streaming_predictions.count_documents({"predicted_sentiment": "positif"})
neutre = db.streaming_predictions.count_documents({"predicted_sentiment": "neutre"})
negatif = db.streaming_predictions.count_documents({"predicted_sentiment": "negatif"})

col1, col2, col3, col4 = st.columns(4)
col1.metric("📦 Total", total)
col2.metric("😊 Positif", positif)
col3.metric("😐 Neutre", neutre)
col4.metric("😠 Négatif", negatif)

# Graphe temps réel
st.subheader("⚡ Ordre des avis vs Sentiment")
recent = list(db.streaming_predictions.find(
    {}, {"order": 1, "sentiment_value": 1, "predicted_sentiment": 1, "text": 1}
).sort("order", -1).limit(100))

if recent:
    df_plot = pd.DataFrame(recent)
    df_plot = df_plot.sort_values("order")
    st.line_chart(df_plot.set_index("order")["sentiment_value"])

    st.subheader("Derniers avis analysés")
    rows = []
    for doc in recent[:10]:
        emoji = "😊" if doc["predicted_sentiment"] == "positif" else \
                "😐" if doc["predicted_sentiment"] == "neutre" else "😠"
        rows.append({
            "N°": doc.get("order", ""),
            "Sentiment": f"{emoji} {doc['predicted_sentiment']}",
            "Texte": doc.get("text", "")[:80] + "..."
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

st.caption(f"Mis à jour : {pd.Timestamp.now().strftime('%H:%M:%S')}")
time.sleep(2)
st.rerun()
