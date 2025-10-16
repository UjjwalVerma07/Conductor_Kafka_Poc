FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY worker.py .
COPY processing_task_worker.py .
COPY minio_client.py .

# Run the worker
CMD ["python", "-u", "worker.py"]

