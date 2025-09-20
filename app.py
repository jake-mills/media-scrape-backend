# app.py
"""
Main FastAPI app for Media Scrape Backend
"""

import os
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from airtable_client import insert_row, find_by_source_url
from providers.openverse import scrape_openverse
# from providers.youtube import scrape_youtube  # placeholder for YouTube provider

# --- Security Key ---
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY")

# --- FastAPI app ---
app = FastAPI(title="Media Scrape Backend", version="1.0.0")


# --- Models ---
class ScrapeRequest(BaseModel):
    topic: str
    searchDates: Optional[str] = None
    targetCount: int = 10
    providers: List[str] = []
    mediaMode: str = "Both"  # Images | Videos | Both
    runId: Optional[str] = None


class ScrapeResponse(BaseModel):
    inserted: int
    skipped: int
    details: List[Dict[str, Any]]


# --- Health endpoints ---
@app.get("/health")
async def health_get():
    return {"ok": True}


@app.head("/health")
async def health_head():
    return Response(status_code=200)


# --- Root silence (avoid 405 noise) ---
@app.get("/")
async def root():
    return {"status": "Media Scrape Backend is live"}


# --- Scraping route ---
@app.post("/scrape-and-insert", response_model=ScrapeResponse)
async def scrape_and_insert(
    req: ScrapeRequest,
    x_shortcuts_key: str = Header(None, alias="X-Shortcuts-Key"),
):
    # --- Auth check ---
    if not SHORTCUTS_KEY or x_shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing X-Shortcuts-Key")

    inserted, skipped = 0, 0
    details: List[Dict[str, Any]] = []

    for provider in req.providers:
        if provider.lower() == "openverse":
            items = await scrape_openverse(
                topic=req.topic,
                search_dates=req.searchDates,
                media_mode=req.mediaMode,
                target_count=req.targetCount,
                run_id=req.runId,
            )
        elif provider.lower() == "youtube":
            # items = await scrape_youtube(...)
            items = []  # placeholder until YouTube provider ready
        else:
            continue

        for item in items:
            # Deduplication: check if Source_URL already exists
            if find_by_source_url(item["Source_URL"]):
                skipped += 1
                continue

            insert_row(item)
            inserted += 1
            details.append(item)

    return ScrapeResponse(inserted=inserted, skipped=skipped, details=details)
