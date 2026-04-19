FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Pre-fetch Docling models at build time so the first request doesn't stall.
# Docling caches layout/OCR models under ~/.cache/docling by default.
RUN python -c "from docling.document_converter import DocumentConverter; DocumentConverter()" || true

COPY . .

ENV BACKEND_PORT=8010

EXPOSE ${BACKEND_PORT}

HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -f http://localhost:${BACKEND_PORT}/api/health || exit 1

CMD ["python", "main.py"]
