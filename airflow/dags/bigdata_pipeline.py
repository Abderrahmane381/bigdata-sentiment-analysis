from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import time

default_args = {
    'owner': 'abderrahmane',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def wait_for_kafka():
    """Attendre que Kafka soit prêt"""
    import subprocess
    max_retries = 10
    for i in range(max_retries):
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 
             '/opt/kafka/bin/kafka-topics.sh',
             '--list', '--bootstrap-server', 'localhost:9092'],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print("✅ Kafka est prêt !")
            return True
        print(f"⏳ Kafka pas encore prêt... tentative {i+1}/{max_retries}")
        time.sleep(10)
    raise Exception("❌ Kafka n'est pas prêt après 100 secondes")

def wait_for_model():
    """Attendre que le modèle soit entraîné"""
    import subprocess
    max_retries = 30
    for i in range(max_retries):
        result = subprocess.run(
            ['docker', 'exec', 'mongodb', 'mongosh',
             'amazon_sentiment', '--eval',
             'db.model_metrics.countDocuments()'],
            capture_output=True, text=True
        )
        if '1' in result.stdout:
            print("✅ Modèle entraîné et stocké dans MongoDB !")
            return True
        print(f"⏳ Modèle pas encore prêt... tentative {i+1}/{max_retries}")
        time.sleep(20)
    raise Exception("❌ Modèle non disponible après 600 secondes")

with DAG(
    dag_id='bigdata_sentiment_pipeline',
    default_args=default_args,
    description='Pipeline Big Data Sentiment Analysis',
    schedule=None,  # Lancement manuel
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bigdata', 'kafka', 'spark', 'mongodb']
) as dag:

    # Task 1 — Démarrer les containers
    start_containers = BashOperator(
    task_id='start_containers',
    bash_command='cd ~/bigdata_project && docker compose up -d mongodb kafka spark_streaming producer dashboard dashboard_offline',
)

    # Task 2 — Attendre Kafka
    wait_kafka = PythonOperator(
        task_id='wait_for_kafka',
        python_callable=wait_for_kafka,
    )

    # Task 3 — Attendre le modèle entraîné
    wait_model = BashOperator(
        task_id='wait_for_model',
        bash_command='''
        echo "Attente du modèle..."
        for i in $(seq 1 60); do
            COUNT=$(docker exec mongodb mongosh amazon_sentiment --eval "db.model_metrics.countDocuments()" --quiet 2>/dev/null | tail -1)
            if [ "$COUNT" = "1" ]; then
                echo "Modèle prêt ✅"
                exit 0
            fi
            echo "Tentative $i/60 - pas encore prêt..."
            sleep 20
        done
        echo "Timeout ❌"
        exit 1
        ''',
        execution_timeout=None,
    )

    # Task 4 — Relancer le producer
    start_producer = BashOperator(
        task_id='start_producer',
        bash_command='docker restart producer',
    )

    start_containers >> wait_kafka >> wait_model >> start_producer
