FROM python:3.12-slim

WORKDIR /app
ARG CACHEBUST=1

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir --upgrade -r requirements.txt \
 && python - <<'PY'
import fastapi, pydantic
print("FASTAPI_VERSION", fastapi.__version__)
print("PYDANTIC_VERSION", pydantic.__version__)
assert pydantic.__version__.split('.')[0] == '2', "Pydantic v2 required"
PY

COPY . .
ENV PORT=8080
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","-w","1","-b","0.0.0.0:8080","app:app"]
