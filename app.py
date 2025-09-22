# app.py
from __future__ import annotations

import os
import logging
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Pydantic v2
from pydantic import BaseModel, Field, field_validator

# Local modules
from airtable_client import upsert_items
from providers.openverse import search_openverse_images

# -----------------------------------------------------------------------------
# Logging / App
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("media-scrape-backend")

app = FastAPI(title="Media Scrape Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten if you need
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
if not SHORTCUTS_KEY:
    log.warning("Environment variable SHORTCUTS_KEY is not set. "
                "Requests to /scrape-and-insert will fail until it is configured.")

# -----------------------------------------------------------------------------
# Models (Pydantic v2)
# -----------------------------------------------------------------------------
MediaMode = Literal["Images", "Videos"]
SupportedProvider = Literal["Openverse"]  # extend this when you add more providers

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="What to search for")
    targetCount: int = Field(ge=1, le=100, description="How many new rows to insert (max 100)")
    providers: List[SupportedProvider] = Field(..., description='Providers to use, e.g. ["Openverse"]')
    mediaMode: MediaMode = Field(..., description='"Images" or "Videos"')
    searchDates: Optional[str] = Field(None, description='Optional date hint like "2000-2010"')
    runId: Optional[str] = Field(None, description="Optional client run id / trace id")

    @field_validator("topic")
    @classmethod
    def topic_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic cannot be blank")
        return v.strip()

class InsertEcho(BaseModel):
    id: Optional[str] = None
    fields: Dict[str, Any] = {}
    source_url: Optional[str] = None

class ScrapeSummary(BaseModel):
    runId: Optional[str] = None
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    skippedCount: int
    inserted: List[InsertEcho] = []

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def ensure_header_key(x_shortcuts_key: str) -> None:
    if not SHORTCUTS_KEY:
        raise HTTPException(
            status_code=500,
            detail="Server missing SHORTCUTS_KEY env var.",
        )
    if x_shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Shortcuts-Key.")

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.post("/scrape-and-insert", response_model=ScrapeSummary)
async def scrape_and_insert(
    body: ScrapeRequest,
    x_shortcuts_key: str = Header(..., alias="X-Shortcuts-Key"),
):
    """
    Pipeline:
      1) Authenticate (X-Shortcuts-Key)
      2) Collect normalized items from requested provider(s)
      3) Upsert into Airtable (de-dup by Source URL), honoring targetCount
      4) Return a summary
    """
    ensure_header_key(x_shortcuts_key)

    # Collect normalized items from providers
    items: List[Dict[str, Any]] = []

    for provider in body.providers:
        # Today we support Openverse for Images.
        if provider == "Openverse":
            if body.mediaMode != "Images":
                raise HTTPException(
                    status_code=400,
                    detail='Openverse currently supports mediaMode="Images" only.',
                )
            try:
                # search_openverse_images returns a list of NormalizedItem dicts
                batch = await search_openverse_images(
                    topic=body.topic,
                    target_count=max(10, body.targetCount),  # overfetch a bit for de-dup
                    license_type="commercial",               # tune as you wish
                )
                items.extend(batch)
            except Exception as e:
                log.exception("Openverse provider failed: %s", e)
                raise HTTPException(status_code=502, detail=f"Provider Openverse failed: {e}")
        else:
            # Shouldn't happen with the current SupportedProvider Literal,
            # but keep a guard for future extensions:
            raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    if not items:
        # No results from providers; short-circuit
        return ScrapeSummary(
            runId=body.runId,
            requestedTarget=body.targetCount,
            providers=body.providers,
            mediaMode=body.mediaMode,
            insertedCount=0,
            skippedCount=0,
            inserted=[],
        )

    # Upsert into Airtable (this function also does de-dup by "Source URL")
    try:
        summary = upsert_items(
            items,
            run_id=body.runId or "",
            search_topics_used=body.topic,
            search_dates_used=body.searchDates or "",
            target_count=body.targetCount,
        )
    except Exception as e:
        log.exception("Airtable upsert_items failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Airtable error: {e}")

    # Build response
    return ScrapeSummary(
        runId=body.runId,
        requestedTarget=body.targetCount,
        providers=body.providers,
        mediaMode=body.mediaMode,
        insertedCount=int(summary.get("insertedCount", 0)),
        skippedCount=int(summary.get("skippedCount", 0)),
        inserted=[InsertEcho(**rec) for rec in summary.get("inserted", [])],
    )

# -----------------------------------------------------------------------------
# Local dev entry (ignored by Render/Gunicorn)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
