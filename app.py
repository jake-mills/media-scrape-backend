# app.py
"""
Media Scrape Backend – FastAPI
Populates Airtable "Videos & Images" with non-duplicates from Openverse.
"""

from __future__ import annotations

import os
from typing import List, Optional, Literal, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from airtable_client import insert_row, find_by_source_url
from providers.openverse import fetch_openverse  # Images & Videos

# -------------------------------------------------------------------
# Config / Security
# Accept either SHORTCUTS_KEY or SHORTCUTS_API_KEY (render.yaml had the latter)
SHORTCUTS_KEY = (os.getenv("SHORTCUTS_KEY") or os.getenv("SHORTCUTS_API_KEY") or "").strip()

# -------------------------------------------------------------------
# App
app = FastAPI(title="Media Scrape Backend", version="1.0.0")

# -------------------------------------------------------------------
# Models (Pydantic v2)
MediaMode = Literal["Images", "Videos", "Both"]

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Search topic/keywords.")
    searchDates: Optional[str] = Field(None, description="Free-text date range, e.g. '2000-2010'.")
    targetCount: int = Field(1, ge=1, le=50, description="Max number of unique rows to insert.")
    providers: List[str] = Field(..., description="e.g., ['Openverse']")
    mediaMode: MediaMode = Field(..., description="'Images', 'Videos', or 'Both'")
    runId: Optional[str] = Field(None, description="Echoed into Airtable 'Run ID'.")

class ScrapeResponse(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    skippedCount: int
    inserted: List[Dict[str, Any]]

# -------------------------------------------------------------------
# Health & Root
@app.get("/health")
async def health_get():
    return {"status": "ok"}

@app.head("/health")
async def health_head():
    return Response(status_code=200)

@app.get("/")
async def root_ok():
    return {"service": "media-scrape-backend", "ok": True}

@app.head("/")
async def root_head():
    return Response(status_code=200)

# -------------------------------------------------------------------
# Main endpoint
@app.post("/scrape-and-insert", response_model=ScrapeResponse)
async def scrape_and_insert(
    req: ScrapeRequest,
    x_shortcuts_key: Optional[str] = Header(None, alias="X-Shortcuts-Key"),
):
    # ---- Auth
    if not SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Service not configured (SHORTCUTS_KEY).")
    if (x_shortcuts_key or "").strip() != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Forbidden (bad X-Shortcuts-Key).")

    run_id = req.runId or ""
    requested_target = req.targetCount

    # ---- Determine media modes to fetch
    want_images = req.mediaMode in ("Images", "Both")
    want_videos = req.mediaMode in ("Videos", "Both")

    # ---- Call providers
    active_providers: List[str] = []
    candidates: List[Dict[str, Any]] = []

    for p in req.providers:
        pl = (p or "").strip().lower()
        if pl == "openverse":
            active_providers.append("Openverse")
            if want_images:
                candidates += await fetch_openverse(
                    topic=req.topic,
                    license_type="commercial",
                    page_size=max(1, requested_target * 3),
                    media="image",
                )
            if want_videos:
                candidates += await fetch_openverse(
                    topic=req.topic,
                    license_type="commercial",
                    page_size=max(1, requested_target * 3),
                    media="video",
                )
        # (future: elif pl == "youtube": ...)

    inserted_rows: List[Dict[str, Any]] = []
    skipped = 0

    for item in candidates:
        # Normalize into your exact Airtable schema
        fields: Dict[str, Any] = {
            "Index": None,  # Autonumber in Airtable
            "Media Type": item.get("Media Type") or ("Images" if want_images and not want_videos else "Videos"),
            "Provider": item.get("Provider", "Openverse"),
            "Thumbnail": item.get("Thumbnail", ""),
            "Title": item.get("Title") or req.topic,
            "Source URL": item.get("Source URL") or item.get("source_url") or "",
            "Search Topics Used": req.topic,
            "Search Dates Used": req.searchDates or "",
            "Published/Created": item.get("Published/Created") or item.get("Published") or item.get("created_on") or "",
            "Copyright": item.get("Copyright") or item.get("license") or "",
            "Run ID": run_id,
            "Notes": item.get("Notes", ""),
        }

        if not fields["Source URL"]:
            continue

        # Dedupe on exact "Source URL"
        if find_by_source_url(fields["Source URL"]):
            skipped += 1
            continue

        insert_row(fields)
        inserted_rows.append(fields)
        if len(inserted_rows) >= requested_target:
            break

    return JSONResponse(
        ScrapeResponse(
            runId=run_id,
            requestedTarget=requested_target,
            providers=active_providers or req.providers,
            mediaMode=req.mediaMode,
            insertedCount=len(inserted_rows),
            skippedCount=skipped,
            inserted=inserted_rows,
        ).model_dump()
    )
