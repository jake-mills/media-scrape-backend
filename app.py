# app.py
from __future__ import annotations

import os
import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import httpx

# ---- Local utilities & providers (these modules already exist in your repo) ----
from airtable import airtable_exists_by_source_url, airtable_batch_create
from date_utils import parse_search_dates  # converts "2000-2010" etc. to tuple or None

# Providers
from providers.youtube import fetch_youtube_async           # Videos
from providers.openverse import fetch_openverse_async       # Images
try:
    # Optional; only used if you include "Archive" in providers + "Images" media mode.
    from providers.archive import fetch_archive_async       # Images (optional)
except Exception:  # pragma: no cover
    fetch_archive_async = None  # gracefully skip if not present


# --------------------------- App & middleware ----------------------------------

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="Media Scrape Backend", version="1.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # adjust if you want to lock this down
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SHORTCUTS_API_KEY = os.getenv("SHORTCUTS_API_KEY") or os.getenv("SHORTCUTS_APT_KEY") or os.getenv("SHORTCUTS_KEY")


# ------------------------------ Models -----------------------------------------

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Search topic/keywords")
    searchDates: Optional[str] = Field(None, description="Year range like '2000-2010'")
    targetCount: int = Field(1, ge=1, le=50, description="How many results you want (best effort)")
    providers: Sequence[str] = Field(..., description="e.g., ['YouTube'] or ['Openverse'] or both")
    mediaMode: str = Field(..., description="'Videos', 'Images', or 'Both'")
    runId: Optional[str] = Field(None, description="Client-generated ID for correlation")

class InsertedRow(BaseModel):
    # Echo the minimal shape we return to the client for visibility
    Title: Optional[str] = None
    Provider: Optional[str] = None
    Source_URL: Optional[str] = None

class ScrapeResponse(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: str
    insertedCount: int
    inserted: List[Dict[str, Any]] = []


# -------------------------- Lifespan / shared client ---------------------------

_http: Optional[httpx.AsyncClient] = None

@app.on_event("startup")
async def _startup():
    global _http
    _http = httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=10.0, read=10.0, write=10.0),
        follow_redirects=True,
        headers={"User-Agent": "MediaScrape/1.0 (+https://example.invalid)", "Accept": "application/json"},
    )

@app.on_event("shutdown")
async def _shutdown():
    global _http
    if _http is not None:
        await _http.aclose()
        _http = None


# ----------------------------- Health & root -----------------------------------

@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    return Response(status_code=200)

@app.get("/")
async def root():
    # Friendly message instead of 404 when someone opens the root URL in a browser.
    return {"ok": True, "message": "See /docs for Swagger UI"}


# --------------------------- Helper functions ----------------------------------

def _validate_api_key(shortcuts_key: Optional[str]) -> None:
    """
    Enforce the pre-shared header if an env key is configured.
    """
    if SHORTCUTS_API_KEY:
        provided = (shortcuts_key or "").strip()
        if not provided or provided != SHORTCUTS_API_KEY:
            raise HTTPException(status_code=403, detail="Forbidden")

def _want_videos(media_mode: str) -> bool:
    return media_mode.lower() in ("videos", "both")

def _want_images(media_mode: str) -> bool:
    return media_mode.lower() in ("images", "both")

def _is_truthy_provider(name: str, providers: Sequence[str]) -> bool:
    return any(p.lower() == name.lower() for p in providers)


# ------------------------------- Main endpoint ---------------------------------

@app.post("/scrape-and-insert", response_model=ScrapeResponse)
async def scrape_and_insert(
    body: ScrapeRequest,
    request: Request,
    x_shortcuts_key: Optional[str] = Header(None, convert_underscores=False),
):
    """
    Scrape from selected providers, normalize, de-dupe against Airtable by Source URL,
    and insert new rows. Returns a summary of what was inserted.
    """
    _validate_api_key(x_shortcuts_key)

    if _http is None:
        raise HTTPException(status_code=503, detail="HTTP client not ready")

    run_id = body.runId or "no-run-id"
    topic = (body.topic or "").strip()
    if not topic:
        raise HTTPException(status_code=422, detail="topic is required")

    # Parse the date range once for providers that support it.
    # parse_search_dates can return tuple or None; we keep original string, too.
    search_dates_str = body.searchDates or None
    _ = parse_search_dates(search_dates_str)  # we keep str; providers can re-parse as needed

    target = max(1, min(50, int(body.targetCount or 1)))  # clamp to a sane range

    provider_tasks: List[asyncio.Task] = []
    collected: List[Dict[str, Any]] = []

    # ------------------- Queue provider tasks (concurrently) -------------------

    # YouTube (Videos)
    if _want_videos(body.mediaMode) and _is_truthy_provider("YouTube", body.providers):
        provider_tasks.append(asyncio.create_task(
            fetch_youtube_async(
                topic=topic,
                search_dates=search_dates_str,  # provider can ignore/parse
                target_count=target,
                run_id=run_id,
                client=_http,
            )
        ))

    # Openverse (Images)
    if _want_images(body.mediaMode) and _is_truthy_provider("Openverse", body.providers):
        provider_tasks.append(asyncio.create_task(
            fetch_openverse_async(
                topic=topic,
                search_dates=search_dates_str,
                target_count=target,
                run_id=run_id,
                client=_http,
            )
        ))

    # Internet Archive (optional Images)
    if fetch_archive_async and _want_images(body.mediaMode) and _is_truthy_provider("Archive", body.providers):
        provider_tasks.append(asyncio.create_task(
            fetch_archive_async(
                topic=topic,
                search_dates=search_dates_str,
                target_count=target,
                run_id=run_id,
                client=_http,
            )
        ))

    # If no valid provider was requested, short-circuit
    if not provider_tasks:
        return ScrapeResponse(
            runId=run_id,
            requestedTarget=target,
            providers=list(body.providers),
            mediaMode=body.mediaMode,
            insertedCount=0,
            inserted=[],
        )

    # ------------------------ Gather & flatten provider results ----------------

    results = await asyncio.gather(*provider_tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            log.warning("Provider task failed: %s", r)
            continue
        if isinstance(r, list):
            collected.extend(r)

    # Nothing fetched
    if not collected:
        return ScrapeResponse(
            runId=run_id,
            requestedTarget=target,
            providers=list(body.providers),
            mediaMode=body.mediaMode,
            insertedCount=0,
            inserted=[],
        )

    # ---------------------- De-dupe by Source URL before insert ----------------

    to_insert: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for row in collected:
        # Expect providers to normalize "Source URL" (or "Source URL" exact key)
        src = row.get("Source URL") or row.get("Source_URL") or row.get("Source") or ""
        src = (src or "").strip()
        if not src:
            continue

        # memory de-dupe to avoid checking same URL multiple times
        if src in seen:
            continue
        seen.add(src)

        # Airtable de-dupe (skip if already present)
        try:
            exists = await airtable_exists_by_source_url(src)
        except Exception as e:
            log.warning("Airtable exists check failed (%s). Skipping row.", e)
            continue

        if not exists:
            # Normalize key to your exact Airtable column names
            to_insert.append({
                "Title": row.get("Title") or "",
                "Provider": row.get("Provider") or "",
                "Media Type": row.get("Media Type") or "",
                "Source URL": src,
                "Thumbnail": row.get("Thumbnail") or {},
                "Published/Created": row.get("Published/Created") or "",
                "Copyright": row.get("Copyright") or "",
                # add any other mapped fields here if your base expects them
                "Search Topics Used": topic,
            })

        # Stop once we have at least target rows queued
        if len(to_insert) >= target:
            break

    # ------------------------------- Batch insert ------------------------------

    inserted_records: List[Dict[str, Any]] = []
    if to_insert:
        try:
            inserted_records = await airtable_batch_create(to_insert)
        except Exception as e:
            log.warning("Airtable batch create failed: %s", e)
            inserted_records = []

    # Echo back a minimal view of inserted rows
    light = []
    for r in inserted_records:
        light.append({
            "Title": r.get("fields", {}).get("Title") or "",
            "Provider": r.get("fields", {}).get("Provider") or "",
            "Source_URL": r.get("fields", {}).get("Source URL") or "",
        })

    return ScrapeResponse(
        runId=run_id,
        requestedTarget=target,
        providers=list(body.providers),
        mediaMode=body.mediaMode,
        insertedCount=len(inserted_records),
        inserted=light,
    )
