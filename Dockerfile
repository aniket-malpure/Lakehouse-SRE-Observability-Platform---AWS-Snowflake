FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY src/ ./src/
COPY config/ ./config/

WORKDIR /app/src

# Prometheus metrics port
EXPOSE 8000

CMD ["python", "pipeline_monitor.py"]
