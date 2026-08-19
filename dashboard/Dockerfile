FROM python:3.12-slim
WORKDIR /app
RUN pip install streamlit pymongo pandas websockets
COPY dashboard.py .
CMD ["streamlit", "run", "dashboard.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
