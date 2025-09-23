FROM python:3.12-slim

ARG CACHEBUST=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && python - <<'PY'
import fastapi, pydantic, sys
print("FASTAPI", fastapi.__version__)
print("PYDANTIC", pydantic.__version__)
assert pydantic.__version__.split('.')[0] == '2', "Pydantic v2 required"
PY

COPY . .
ENV PORT=8080
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","-w","2","-b","0.0.0.0:8080","app:app"]
