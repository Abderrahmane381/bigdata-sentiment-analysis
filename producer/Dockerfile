FROM python:3.12-slim
WORKDIR /app
RUN pip install kafka-python pymongo
COPY producer.py .
CMD ["python", "producer.py"]
