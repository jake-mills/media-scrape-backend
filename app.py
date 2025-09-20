# app.py
import os
import logging
from typing import List, Optional

import aiohttp
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse, Response

from airtable_client import AirtableClient
from providers.openverse import fetch_openverse_async

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("app")

# ------------------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "")
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY")
DEBUG_OPENVERSE = os.getenv("DEBUG_OPENVERSE")

if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
    log.warning("One or more Airtable environment variables are missing.")

if not SHORTCUTS_KEY:
    log.warning("SHORTCUTS_KEY is not set; all requests will be rejected (403).")

# Optional verbose logs from the Openverse provider
if DEBUG_OPENVERSE:
    logging.getLogger("providers.openverse").setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# FastAPI app & middleware
# ------------------------------------------------------------------------------
app = FastAPI(title="Media Scrape Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # lock down if you prefer
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared resources
app.state.http_session: Optional[aiohttp.ClientSession] = None
app.state.airtable: Optional[AirtableClient] = None

@app.on_event("startup")
async def _startup():
    # Single shared HTTP session for the process
    app.state.http_session = aiohttp.ClientSession()

    # Single shared Airtable client that reuses the shared session
    app.state.airtable = AirtableClient(
        base_id=AIRTABLE_BASE_ID,
        table_name=AIRTABLE_TABLE_NAME,
        api_key=AIRTABLE_API_KEY,
        session=app.state.http_session,
    )
    log.info("Startup complete.")

@app.on_event("shutdown")
async def _shutdown():
    if app.state.airtable:
        await app.state.airtable.close()
    if app.state.http_session and not app.state.http_session.closed:
        await app.state.http_session.close()
    log.info("Shutdown complete.")

# ------------------------------------------------------------------------------
# Models
# ------------------------------------------------------------------------------
class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Topic/keywords to search.")
    searchDates: Optional[str] = Field(
        None,
        description="Optional year range like '2000-2010' (provider will interpret best-effort).",
    )
    targetCount: int = Field(1, ge=1, le=50, description="Max records to insert.")
    providers: List[str] = Field(..., description="e.g., ['Openverse'] or ['YouTube'].")
    mediaMode: str = Field(..., description="'Images' or 'Videos'")
    runId: str = Field(..., description="Caller-supplied id for traceability.")

# ------------------------------------------------------------------------------
# Auth dependency (Apple Shortcuts)
# ------------------------------------------------------------------------------
async def require_shortcuts_key(x_shortcuts_key: Optional[str] = Header(None)):
    if not SHORTCUTS_KEY:
        # Service was started without a SHORTCUTS_KEY – reject everything
        raise HTTPException(status_code=403, detail="Service not configured (SHORTCUTS_KEY).")
    if x_shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Forbidden (invalid X-Shortcuts-Key).")

# ------------------------------------------------------------------------------
# Health + root (silence 404/405 noise after deploy)
# ------------------------------------------------------------------------------
@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    return Response(status_code=200)

@app.get("/")
async def root_ok():
    return {"service": "media-scrape-backend", "ok": True}

@app.head("/")
async def root_head():
    return Response(status_code=200)

# ------------------------------------------------------------------------------
# Main endpoint
# ------------------------------------------------------------------------------
@app.post("/scrape-and-insert")
async def scrape_and_insert(
    payload: ScrapeRequest,
    _: None = Depends(require_shortcuts_key),
):
    """
    Orchestrates provider fetch -> de-dup -> insert into Airtable.
    """
    if app.state.http_session is None or app.state.airtable is None:
        # Should not happen in normal operation, but guards against partial startup.
        raise HTTPException(status_code=500, detail="Service not ready (startup incomplete).")

    run_id = payload.runId
    topic = (payload.topic or "").strip()
    providers = [p.strip() for p in payload.providers]
    media_mode = (payload.mediaMode or "").strip()
    target = int(payload.targetCount)

    if not topic:
        raise HTTPException(status_code=422, detail="Topic cannot be empty.")

    items: List[dict] = []

    # --- Providers ---
    for provider in providers:
        try:
            if provider.lower() == "openverse" and media_mode.lower() == "images":
                fetched = await fetch_openverse_async(
                    query=topic,
                    search_dates=payload.searchDates,
                    license_type="commercial",
                    page_size=max(1, target),
                    session=app.state.http_session,
                    run_id=run_id,
                )
                items.extend(fetched)

            # elif provider.lower() == "youtube" and media_mode.lower() == "videos":
            #     fetched = await fetch_youtube_async(...)
            #     items.extend(fetched)

        except Exception as e:
            # Do not fail the whole run because one provider hiccuped
            log.warning("Provider %s failed: %s", provider, e)

    # --- Deduplicate by Source_URL against Airtable and INSERT ---
    inserted: List[dict] = []
    for item in items:
        source_url = item.get("source_url")
        if not source_url:
            continue

        # IMPORTANT: this must be awaited (fixes 'was never awaited' + 0-insert behavior)
        exists = await app.state.airtable.exists_by_source_url(source_url)
        if exists:
            continue

        fields = {
            "Title": (item.get("title") or topic).strip(),
            "Provider": (item.get("provider") or "Openverse"),
            "Source_URL": source_url,
        }

        await app.state.airtable.insert_record(fields)
        inserted.append(fields)

        if len(inserted) >= target:
            break

    return JSONResponse(
        {
            "runId": run_id,
            "requestedTarget": target,
            "providers": providers,
            "mediaMode": media_mode,
            "insertedCount": len(inserted),
            "inserted": inserted,
        }
    )
