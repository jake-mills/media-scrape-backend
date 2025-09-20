# app.py
"""
Media Scrape Backend – FastAPI
Populates Airtable "Videos & Images" with non-duplicates from Openverse.
"""

import os
from typing import List, Optional, Literal, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from airtable_client import insert_row, find_by_source_url
from providers.openverse import fetch_openverse  # rich fields for Images/Videos

# -------------------------------------------------------------------
# Config / Security
# -------------------------------------------------------------------
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()

# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------
app = FastAPI(title="Media Scrape Backend", version="1.0.0")

# -------------------------------------------------------------------
# Models (Pydantic v2)
# -------------------------------------------------------------------
MediaMode = Literal["Images", "Videos", "Both"]

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Search topic/keywords.")
    searchDates: Optional[str] = Field(None, description="Free-text date range (e.g., '2000-2010').")
    targetCount: int = Field(1, ge=1, le=50, description="How many unique rows to insert at most.")
    providers: List[str] = Field(..., description="e.g., ['Openverse']")
    mediaMode: MediaMode = Field(..., description="'Images', 'Videos', or 'Both'")
    runId: Optional[str] = Field(None, description="Client-supplied run id, echoed to Airtable 'Run ID'.")

class InsertedRow(BaseModel):
    # Mirrors Airtable fields for the 'Videos & Images' table
    Index: Optional[int] = None  # Autonumber (not sent to Airtable)
    Media_Type: str
    Provider: str
    Thumbnail: str
    Title: str
    Source_URL: str
    Search_Topics_Used: str
    Search_Dates_Used: str
    Published_Created: str
    Copyright: str
    Run_ID: str
    Notes: str

class ScrapeResponse(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    skippedCount: int
    inserted: List[Dict[str, Any]]  # echo of Airtable field dicts


# -------------------------------------------------------------------
# Health & Root
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# Main endpoint
# -------------------------------------------------------------------
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

    # ---- Currently support Openverse
    providers = [p for p in req.providers if p.lower() == "openverse"]
    if not providers:
        return ScrapeResponse(
            runId=run_id,
            requestedTarget=requested_target,
            providers=req.providers,
            mediaMode=req.mediaMode,
            insertedCount=0,
            skippedCount=0,
            inserted=[],
        )

    # ---- Fetch from Openverse (Images and/or Videos)
    candidates: List[Dict[str, Any]] = []
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

    # ---- Deduplicate by Airtable "Source URL" and insert
    inserted_rows: List[Dict[str, Any]] = []
    skipped = 0

    for item in candidates:
        # Build the Airtable record using EXACT field names from your table schema
        fields: Dict[str, Any] = {
            "Media Type": item.get("Media Type", "Images" if want_images and not want_videos else "Videos"),
            "Provider": item.get("Provider", "Openverse"),
            "Thumbnail": item.get("Thumbnail", ""),
            "Title": item.get("Title") or req.topic,
            "Source URL": item.get("Source URL") or item.get("Source_URL") or item.get("source_url") or "",
            "Search Topics Used": req.topic,
            "Search Dates Used": req.searchDates or "",
            "Published/Created": item.get("Published/Created") or item.get("Published") or item.get("created_on") or "",
            "Copyright": item.get("Copyright") or item.get("license") or "",
            "Run ID": run_id,
            "Notes": item.get("Notes", ""),
        }

        # Must have a URL to dedupe/insert
        if not fields["Source URL"]:
            continue

        # De-dup check: field name EXACTLY "Source URL"
        if find_by_source_url(fields["Source URL"], field="Source URL"):
            skipped += 1
            continue

        # Insert into Airtable
        insert_row(fields)
        inserted_rows.append(fields)
        if len(inserted_rows) >= requested_target:
            break

    return JSONResponse(
        ScrapeResponse(
            runId=run_id,
            requestedTarget=requested_target,
            providers=providers,
            mediaMode=req.mediaMode,
            insertedCount=len(inserted_rows),
            skippedCount=skipped,
            inserted=inserted_rows,
        ).model_dump()
    )
