FROM python:3.12-slim
WORKDIR /app
RUN pip install streamlit pymongo pandas
COPY dashboard_offline.py .
CMD ["streamlit", "run", "dashboard_offline.py", "--server.port", "8502", "--server.address", "0.0.0.0"]
