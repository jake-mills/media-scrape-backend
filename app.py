# app.py
import os
import logging
from typing import List, Optional

import aiohttp
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse, Response

from airtable_client import AirtableClient
from providers.openverse import fetch_openverse_async


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
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

if DEBUG_OPENVERSE:
    logging.getLogger("providers.openverse").setLevel(logging.INFO)


# ------------------------------------------------------------------------------
# FastAPI app & lifespan (manage shared session/client)
# ------------------------------------------------------------------------------
app = FastAPI(title="Media Scrape Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust to your origins if you want to lock it down
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared resources
app.state.http_session = None
app.state.airtable = None


@app.on_event("startup")
async def _startup():
    app.state.http_session = aiohttp.ClientSession()
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
    searchDates: Optional[str] = Field(None, description="Human string like '2000–2010' (not enforced upstream).")
    targetCount: int = Field(1, ge=1, le=50, description="How many records to insert at most.")
    providers: List[str] = Field(..., description="e.g. ['Openverse'] or ['YouTube'].")
    mediaMode: str = Field(..., description="'Images' or 'Videos'")
    runId: str = Field(..., description="Caller-supplied id for traceability.")


# ------------------------------------------------------------------------------
# Auth dependency
# ------------------------------------------------------------------------------
async def require_shortcuts_key(x_shortcuts_key: Optional[str] = Header(None)):
    if not SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Service not configured (SHORTCUTS_KEY).")
    if x_shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Forbidden (invalid X-Shortcuts-Key).")


# ------------------------------------------------------------------------------
# Health + root (silence 405/404 noise)
# ------------------------------------------------------------------------------
@app.get("/health")
async def health_get():
    return {"ok": True}


@app.head("/health")
async def health_head():
    return Response(status_code=200)


@app.get("/")
async def root_ok():
    # Simple landing to avoid a 404/405 after deploy
    return {"service": "media-scrape-backend", "ok": True}


@app.head("/")
async def root_head():
    return Response(status_code=200)


# ------------------------------------------------------------------------------
# Main endpoint
# ------------------------------------------------------------------------------
@app.post("/scrape-and-insert")
async def scrape_and_insert(payload: ScrapeRequest, _: None = Depends(require_shortcuts_key)):
    """
    Orchestrates provider fetch -> de-dup -> insert into Airtable.
    """
    run_id = payload.runId
    topic = payload.topic.strip()
    providers = [p.strip() for p in payload.providers]
    media_mode = payload.mediaMode.strip()
    target = int(payload.targetCount)

    if not topic:
        raise HTTPException(status_code=422, detail="Topic cannot be empty.")

    items: List[dict] = []

    # --- Providers ---
    for provider in providers:
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
        # elif provider == "YouTube" and media_mode == "Videos":
        #     ... add your YouTube fetch here ...

    # --- Deduplicate by Source_URL against Airtable and INSERT ---
    inserted: List[dict] = []
    for item in items:
        source_url = item.get("source_url")
        if not source_url:
            continue

        # ✅ CRITICAL: Await the async duplicate check (this fixes the 0-insert bug)
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

    return {
        "runId": run_id,
        "requestedTarget": target,
        "providers": providers,
        "mediaMode": media_mode,
        "insertedCount": len(inserted),
        "inserted": inserted,
    }
