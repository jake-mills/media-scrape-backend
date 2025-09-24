FROM python:3.12-slim

# Cache-buster to force a fresh pip layer when requirements change
ARG CACHEBUST=1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (tiny)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Runtime port for Cloud Run
ENV PORT=8080

# Gunicorn (Uvicorn worker)
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","-w","2","-b","0.0.0.0:8080","app:app"]
