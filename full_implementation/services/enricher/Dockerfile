FROM python:3.9-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY service.py .
COPY validator.py .

# Run the service
CMD ["python", "-u", "service.py"]

