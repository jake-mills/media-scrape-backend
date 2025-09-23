# Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install deps first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- cache-bust layer: update the file before each build ---
COPY buildstamp.txt /app/.buildstamp

# Now copy the app
COPY . .

ENV PORT=8080

# Start the FastAPI app with Gunicorn+Uvicorn
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "-b", "0.0.0.0:8080", "app:app"]
