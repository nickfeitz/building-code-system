FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV EMBEDDING_MODEL=intfloat/e5-large-v2
ENV EMBEDDING_PORT=8011

EXPOSE ${EMBEDDING_PORT}

HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=30s \
    CMD curl -f http://localhost:${EMBEDDING_PORT}/health || exit 1

CMD ["python", "main.py"]
