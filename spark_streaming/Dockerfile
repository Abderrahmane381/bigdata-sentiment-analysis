FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip install pyspark==4.2.0 pymongo kafka-python nltk numpy requests
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt'); nltk.download('punkt_tab')"

COPY train_model.py .
COPY consumer.py .

CMD ["bash", "-c", "python train_model.py && python consumer.py"]
