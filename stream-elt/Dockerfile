# Use Python 3.9. Slim for small footprint.
FROM python:3.9-slim

# Installs CA certs for TLS (SASL_SSL with Kafka) 
# Installs curl for HTTP requests
# Removes cached package lists to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user for better security
RUN useradd --create-home --uid 1000 appuser
WORKDIR /usr/src/app

# Leverage layer caching: copy only requirements first
COPY requirements.txt ./

# Install Python dependencies (confluent-kafka, websocket-client, python-dotenv, etc.)
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files to the container
COPY . .

# Switch to non-root
USER appuser

# Default runtime env (override via docker compose or env vars)
# Do NOT hardcode secrets here; use .env or environment in compose
ENV PYTHONUNBUFFERED=1 \
    TOPIC_NAME=market_bars \
    FEED=v2/iex
    # For 24/7 testing, switch feed:
    # FEED = "v2/test"
    
# Run your stream producer
CMD ["python", "producer.py"]

