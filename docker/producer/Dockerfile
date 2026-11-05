FROM python:3.12-slim

WORKDIR /app

# Runtime libs sometimes needed by the librdkafka wheel bundled in confluent-kafka.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsasl2-2 libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

USER 1000:1000
ENTRYPOINT ["python", "-u", "producer.py"]
