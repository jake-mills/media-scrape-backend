from fastapi import FastAPI, Request, Response, Header, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import os

# Import your existing routers / logic
from routers import scrape  # adjust if your scrape routes live elsewhere

app = FastAPI(
    title="Media Scrape Backend",
    description="Backend service for scraping videos/images and inserting into Airtable",
    version="1.0.0"
)

# Allow CORS for local development & Shortcuts
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Healthcheck routes ===
@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    return Response(status_code=200)

# === Root route (fix the 404 after deploy) ===
@app.get("/", include_in_schema=False)
async def root():
    # Option A: redirect to Swagger UI
    return RedirectResponse(url="/docs")

    # Option B: simple JSON response instead of redirect
    # return JSONResponse({"ok": True, "hint": "See /docs for API, /health for status"})

# === Include your scrape routes ===
app.include_router(scrape.router)

# === Security check middleware for Shortcuts Key ===
@app.middleware("http")
async def verify_shortcuts_key(request: Request, call_next):
    # Skip health + docs so they always load
    if request.url.path not in ["/health", "/", "/docs", "/openapi.json"]:
        expected_key = os.getenv("SHORTCUTS_KEY")
        provided_key = request.headers.get("X-Shortcuts-Key")
        if expected_key and provided_key != expected_key:
            raise HTTPException(status_code=403, detail="Forbidden: invalid X-Shortcuts-Key")
    return await call_next(request)
