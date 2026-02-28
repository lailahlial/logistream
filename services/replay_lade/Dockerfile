FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.3.0
COPY main.py .
CMD ["python", "main.py"]
