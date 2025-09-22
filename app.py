import os
import logging
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from providers.openverse import search_openverse_images
from airtable_client import insert_row, find_by_source_url

# -------------------------------------------------
# App setup
# -------------------------------------------------
app = FastAPI(title="Media Scrape Backend", version="1.0.0")
log = logging.getLogger("media-scrape-backend")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY")

# -------------------------------------------------
# Models
# -------------------------------------------------
MediaMode = Literal["Images", "Videos"]
ProviderName = Literal["Openverse"]  # extend as you add providers

class ScrapeRequest(BaseModel):
    topic: str = Field(..., min_length=1)
    targetCount: int = Field(..., ge=1, le=50)
    providers: List[ProviderName]
    mediaMode: MediaMode
    runId: str = Field(..., min_length=1)

    # NEW: Optional metadata you wanted to store verbatim in Airtable
    searchTopics: Optional[str] = None
    searchDates: Optional[str] = None

class InsertedItem(BaseModel):
    title: Optional[str] = None
    provider: Optional[str] = None
    source_url: Optional[str] = None
    thumbnailURL: Optional[str] = None
    copyright: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    tags: Optional[str] = None

class ScrapeResult(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    skippedCount: int
    inserted: List[InsertedItem]

# -------------------------------------------------
# Health
# -------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def require_shortcuts_key(header_value: Optional[str]):
    if not SHORTCUTS_KEY:
        log.warning("SHORTCUTS_KEY not set in environment; refusing all writes.")
        raise HTTPException(status_code=500, detail="Server not configured.")
    if header_value != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized.")

# -------------------------------------------------
# Main route
# -------------------------------------------------
@app.post("/scrape-and-insert", response_model=ScrapeResult)
async def scrape_and_insert(
    payload: ScrapeRequest,
    x_shortcuts_key: Optional[str] = Header(None, convert_underscores=False),
):
    # Auth
    require_shortcuts_key(x_shortcuts_key)

    # Only Openverse for now (your other providers can be added later)
    all_new: List[Dict[str, Any]] = []
    skipped = 0

    for provider in payload.providers:
        if provider == "Openverse":
            try:
                batch = await search_openverse_images(
                    query=payload.topic,
                    count=payload.targetCount * 4,  # over-fetch to allow filtering/dedup
                )
            except Exception as exc:
                # surface a friendly message (keeps your 2xx schema stable)
                msg = f"Provider Openverse failed: {exc}"
                log.error(msg)
                raise HTTPException(status_code=502, detail=msg)

            # write/skip with Airtable de-dup by Source URL
            for idx, item in enumerate(batch):
                if len(all_new) >= payload.targetCount:
                    break

                # Map into your Airtable columns exactly as you requested
                fields_for_airtable = {
                    "Index": idx + 1,  # per-run index
                    "Media Type": "Image" if payload.mediaMode == "Images" else "Video",
                    "Provider": "Openverse",
                    "Thumbnail": item.get("thumbnailURL"),
                    "Title": item.get("title"),
                    "Source URL": item.get("source_url"),
                    "Search Topics Used": payload.searchTopics,
                    "Search Dates Used": payload.searchDates,
                    "Published/Created": item.get("published") or item.get("created"),
                    "Copyright": item.get("copyright"),
                    "Run ID": payload.runId,
                    "Notes": None,
                }

                # If Source URL missing, skip early
                src = fields_for_airtable["Source URL"]
                if not src:
                    continue

                # de-dup by Source URL
                existing = find_by_source_url(src)
                if existing:
                    skipped += 1
                    continue

                # insert
                ok = insert_row(fields_for_airtable)
                if ok:
                    all_new.append(
                        {
                            "title": item.get("title"),
                            "provider": "Openverse",
                            "source_url": src,
                            "thumbnailURL": item.get("thumbnailURL"),
                            "copyright": item.get("copyright"),
                            "width": item.get("width"),
                            "height": item.get("height"),
                            "tags": item.get("tags"),
                        }
                    )
                else:
                    skipped += 1
        else:
            # future providers can be added here
            log.info("Provider %s is not implemented yet; skipping.", provider)
            continue

    return ScrapeResult(
        runId=payload.runId,
        requestedTarget=payload.targetCount,
        providers=payload.providers,
        mediaMode=payload.mediaMode,
        insertedCount=len(all_new),
        skippedCount=skipped,
        inserted=[InsertedItem(**row) for row in all_new],
    )
