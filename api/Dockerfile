FROM python:3.12-slim
WORKDIR /app
RUN pip install fastapi uvicorn
COPY api.py .
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
