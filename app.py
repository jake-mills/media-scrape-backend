# app.py
from __future__ import annotations

import os
import asyncio
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from airtable_client import find_by_source_url, insert_row
from providers.openverse import OpenverseClient
try:
    from providers.youtube import YouTubeClient  # optional
except Exception:
    YouTubeClient = None  # if you don't use YouTube, this is fine

APP_VERSION = "1.0.0"

# ---------- Env ----------
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Videos & Images").strip()
OPENVERSE_TOKEN = os.getenv("OPENVERSE_TOKEN", "").strip()

for key in ("AIRTABLE_API_KEY", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_NAME"):
    if not os.getenv(key):
        raise RuntimeError(f"Missing required env var: {key}")

# ---------- App ----------
app = FastAPI(title="Media Scrape Backend", version=APP_VERSION)

@app.exception_handler(Exception)
async def unhandled_exception_handler(_, exc: Exception):
    return JSONResponse(status_code=500, content={"error": "internal_error", "detail": str(exc)})

@app.get("/health")
async def health_get():
    return {"status": "ok", "version": APP_VERSION}

@app.head("/health")
async def health_head():
    return Response(status_code=200)

@app.get("/")
async def root():
    return {"ok": True, "version": APP_VERSION}

# ---------- Models ----------
MediaMode = Literal["Images", "Videos", "Both"]

class ScrapeRequest(BaseModel):
    topic: str
    # the names your Shortcut/curl already send:
    searchTopics: Optional[str] = None
    searchDates: Optional[str] = None
    # optional explicit “Used” fields (if you ever want to override)
    searchTopicsUsed: Optional[str] = None
    searchDatesUsed: Optional[str] = None

    targetCount: int = Field(10, ge=1, le=200)
    providers: List[str] = Field(default_factory=lambda: ["Openverse"])
    mediaMode: MediaMode = "Images"
    runId: Optional[str] = None

class ScrapeResult(BaseModel):
    inserted: int
    skipped_duplicates: int
    provider_counts: Dict[str, int]

def _require_shortcuts_key(any_header_value: Optional[str]):
    """
    Enforce the shared secret if SHORTCUTS_KEY is set in the environment.
    Accepts X-Shortcuts-Key / x_shortcuts_key / SHORTCUTS_KEY headers.
    """
    if not SHORTCUTS_KEY:
        return  # secret disabled -> allow all (useful during local testing)
    if not any_header_value or any_header_value.strip() != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Shortcuts-Key")

# ---------- Route ----------
@app.post("/scrape-and-insert", response_model=ScrapeResult)
async def scrape_and_insert(
    body: ScrapeRequest,
    # Accept common spellings for the header:
    x_shortcuts_key_hyphen: Optional[str] = Header(default=None, alias="X-Shortcuts-Key"),
    x_shortcuts_key_underscore: Optional[str] = Header(default=None, alias="x_shortcuts_key"),
    x_shortcuts_key_plain: Optional[str] = Header(default=None, alias="SHORTCUTS_KEY"),
):
    # pick whichever arrived
    header_key = x_shortcuts_key_hyphen or x_shortcuts_key_underscore or x_shortcuts_key_plain
    _require_shortcuts_key(header_key)

    topic = (body.topic or "").strip()
    if not topic:
        raise HTTPException(status_code=400, detail="topic is required")

    want_images = body.mediaMode in ("Images", "Both")
    want_videos = body.mediaMode in ("Videos", "Both")

    target = max(1, body.targetCount)
    inserted = 0
    skipped = 0
    provider_counts: Dict[str, int] = {}

    # columns you want set from request; prefer explicit “Used”, then raw fields, then topic
    search_topics_used = (body.searchTopicsUsed or body.searchTopics or body.topic or "").strip()
    search_dates_used = (body.searchDatesUsed or body.searchDates or "").strip()

    tasks = []

    # Openverse (images)
    if want_images and any(p.lower() == "openverse" for p in body.providers):
        tasks.append(OpenverseClient(token=OPENVERSE_TOKEN or None).search_images(topic, target))

    # YouTube (videos) - optional
    if want_videos and any(p.lower() == "youtube" for p in body.providers):
        if not YouTubeClient:
            raise HTTPException(status_code=400, detail="YouTube provider unavailable (module not present).")
        tasks.append(YouTubeClient().search_videos(topic, target))

    results = await asyncio.gather(*tasks) if tasks else []

    for provider_name, items in results:
        provider_counts.setdefault(provider_name, 0)
        for item in items:
            fields: Dict[str, Any] = {
                "Media Type": item.get("media_type") or ("Images" if want_images else "Videos"),
                "Provider": provider_name,
                "Thumbnail": item.get("thumbnail") or "",
                "Title": item.get("title") or "",
                "Source URL": item.get("source_url") or "",
                "Search Topics Used": search_topics_used,
                "Search Dates Used": search_dates_used,
                "Published/Created": item.get("published") or "",
                "Copyright": item.get("copyright") or "",
                "Run ID": body.runId or "",
                "Notes": item.get("notes") or "",
            }

            # de-dup by exact Source URL
            src = fields["Source URL"]
            if src and find_by_source_url(src):
                skipped += 1
                continue

            insert_row(fields)
            inserted += 1
            provider_counts[provider_name] += 1

            if inserted >= target:
                break
        if inserted >= target:
            break

    return ScrapeResult(inserted=inserted, skipped_duplicates=skipped, provider_counts=provider_counts)
