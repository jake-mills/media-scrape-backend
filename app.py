# app.py
from __future__ import annotations

import os
import logging
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

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
    allow_origins=["*"],      # tighten if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
# Support BOTH names to match your render.yaml and earlier code
SHORTCUTS_KEY = (
    os.getenv("SHORTCUTS_KEY")
    or os.getenv("SHORTCUTS_API_KEY")
    or ""
).strip()
if not SHORTCUTS_KEY:
    log.warning("SHORTCUTS_KEY/SHORTCUTS_API_KEY not set; /scrape-and-insert will 401.")

# -----------------------------------------------------------------------------
# Models (Pydantic v2)
# -----------------------------------------------------------------------------
MediaMode = Literal["Images", "Videos"]
SupportedProvider = Literal["Openverse"]  # extend later

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Search topic/keywords")
    targetCount: int = Field(ge=1, le=100, description="Max new rows to insert")
    providers: List[SupportedProvider] = Field(..., description='e.g., ["Openverse"]')
    mediaMode: MediaMode = Field(..., description='"Images" or "Videos"')
    searchDates: Optional[str] = Field(None, description='Optional hint like "2000-2010"')
    runId: Optional[str] = Field(None, description="Optional client trace id")

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
def ensure_key(xkey: str) -> None:
    if not SHORTCUTS_KEY or xkey != SHORTCUTS_KEY:
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
    ensure_key(x_shortcuts_key)

    # Collect normalized items from provider(s)
    items: List[Dict[str, Any]] = []
    for prov in body.providers:
        if prov == "Openverse":
            if body.mediaMode != "Images":
                raise HTTPException(
                    status_code=400,
                    detail='Openverse currently supports mediaMode="Images" only.',
                )
            try:
                batch = await search_openverse_images(
                    topic=body.topic,
                    target_count=max(10, body.targetCount),  # overfetch a bit for de-dup
                    license_type="commercial",
                )
                items.extend(batch)
            except Exception as e:
                log.exception("Openverse failed: %s", e)
                raise HTTPException(status_code=502, detail=f"Provider Openverse failed: {e}")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported provider: {prov}")

    if not items:
        return ScrapeSummary(
            runId=body.runId,
            requestedTarget=body.targetCount,
            providers=body.providers,
            mediaMode=body.mediaMode,
            insertedCount=0,
            skippedCount=0,
            inserted=[],
        )

    # Upsert to Airtable (de-dup by "Source URL")
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

    return ScrapeSummary(
        runId=body.runId,
        requestedTarget=body.targetCount,
        providers=body.providers,
        mediaMode=body.mediaMode,
        insertedCount=int(summary.get("insertedCount", 0)),
        skippedCount=int(summary.get("skippedCount", 0)),
        inserted=[InsertEcho(**rec) for rec in summary.get("inserted", [])],
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
