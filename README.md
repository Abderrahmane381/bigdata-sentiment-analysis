# 🛒 Amazon Reviews — Real-Time Sentiment Analysis Pipeline

> A production-style Big Data pipeline for real-time sentiment analysis of Amazon product reviews, combining Kafka, Spark Streaming, MongoDB, Streamlit and Airflow.

---

## 📌 Overview

This project implements a complete end-to-end Big Data pipeline that:
- Streams Amazon reviews in real time via **Kafka**
- Processes and predicts sentiment using **Spark Streaming + ML**
- Stores results in **MongoDB**
- Visualizes predictions in real time via a **Streamlit dashboard**
- Orchestrates the entire pipeline with **Apache Airflow**

---

## 🏗️ Architecture

```
Producer (Amazon Reviews)
        ↓
    Kafka Topic
    (amazon-reviews)
        ↓
  Spark Streaming
  (NLP + ML Prediction)
    ↙           ↘
MongoDB        Dashboard Online
(Storage)      (Real-time visualization)
    ↓
Dashboard Offline
(Stats & Analysis)

All orchestrated by Apache Airflow
```

---

## 🏆 Key Results

| Metric | Value |
|--------|-------|
| Dataset | Amazon Fine Food Reviews (566,803 reviews) |
| Train set | 80% (453,440 reviews) |
| Validation set | 10% (56,588 reviews) |
| Test set | 10% (56,775 reviews) |
| Model | Logistic Regression + TF-IDF |
| Validation Accuracy | **85.45%** |
| Test Accuracy | **85.41%** |

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka 4.3.1 |
| Stream Processing | Apache Spark 4.2.0 (PySpark) |
| Machine Learning | Spark MLlib (Logistic Regression) |
| NLP | NLTK, TF-IDF |
| Database | MongoDB 8.0 |
| Dashboard | Streamlit |
| Orchestration | Apache Airflow 3.2.0 |
| Containerization | Docker + Docker Compose |

---

## 📂 Project Structure

```
├── bigdata_project/
│   ├── docker-compose.yml
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── producer.py          ← Sends reviews to Kafka
│   ├── spark_streaming/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── train_model.py       ← Trains & saves ML model
│   │   └── consumer.py          ← Spark Streaming consumer
│   ├── dashboard/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── dashboard.py         ← Real-time dashboard
│   ├── dashboard_offline/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── dashboard_offline.py ← Offline analytics dashboard
│   └── api/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── api.py               ← FastAPI WebSocket server
├── airflow/
│   └── dags/
│       └── bigdata_pipeline.py  ← Airflow DAG
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Python 3.12
- Docker Desktop (installed and running)
- Apache Airflow 3.2.0

> **Note:** All Python dependencies are already included in each service Dockerfile.
> No manual pip install required — just start the project and Docker handles everything.

### Option 1 — With Airflow (Recommended)

```bash
# Activate Airflow environment
source ~/airflow_env/bin/activate

# Start Airflow
airflow standalone

# Open http://localhost:8080
# Trigger the DAG: bigdata_sentiment_pipeline
```

### Option 2 — Direct Docker Compose

```bash
cd bigdata_project
docker compose up --build
```

### View Dashboards
- **Real-time dashboard** → http://localhost:8501
- **Offline analytics** → http://localhost:8502
- **Airflow UI** → http://localhost:8080

---

## 📊 Airflow DAG

```
start_containers → wait_for_kafka → wait_for_model → start_producer
```

| Task | Description |
|------|-------------|
| start_containers | Launches all Docker services |
| wait_for_kafka | Waits until Kafka is ready |
| wait_for_model | Waits until ML model is trained |
| start_producer | Starts streaming reviews to Kafka |

---

## 🗂️ Dataset

- **Source:** [Amazon Fine Food Reviews](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews)
- **Size:** 568,454 reviews
- **Period:** Oct 1999 – Oct 2012
- **Labels:** Score 1-5 → Negative (<3), Neutral (=3), Positive (>3)

---

## 👤 Author

**Abderrahmane Belkasmi**
Master AI & Data Science
[LinkedIn](www.linkedin.com/in/abderrahmane-belkasmi-ba3b64266)
