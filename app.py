from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import os
import logging

from providers.openverse import search as openverse_search
from airtable_client import (
    find_by_source_url,
    insert_record,  # alias provided in airtable_client for backwards compat
)

APP_VERSION = "1.0.0"

# Env
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "").strip()

app = FastAPI(title="media-scrape-backend", version=APP_VERSION)

# ---------- Models ----------

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Primary search text")
    targetCount: int = Field(1, ge=1, le=50)
    providers: List[str] = Field(default_factory=lambda: ["Openverse"])
    mediaMode: str = Field("Images", description='"Images" or "Audio" (Openverse supports both)')
    runId: Optional[str] = None
    searchTopics: Optional[str] = None
    searchDates: Optional[str] = None


# ---------- Helpers ----------

def normalize_openverse(item: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Openverse record to a consistent schema for Airtable."""
    return {
        "source": "openverse",
        "source_id": item.get("id"),
        "title": item.get("title") or "",
        "url": item.get("url") or "",
        "thumbnail": item.get("thumbnail") or "",
        "provider": "Openverse",
    }


PROVIDER_MAP = {
    "Openverse": ("openverse", openverse_search, normalize_openverse),
    # Add other providers here, e.g. "YouTube": ( "youtube", youtube_search, normalize_youtube )
}


def require_shortcuts_key(key_header: str):
    if not SHORTCUTS_KEY:
        raise HTTPException(status_code=500, detail="Server missing SHORTCUTS_KEY")
    if key_header != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Shortcuts-Key")


def ensure_airtable_env():
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
        raise HTTPException(status_code=500, detail="Airtable env missing")


# ---------- Routes ----------

@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION}

@app.post("/scrape-and-insert")
async def scrape_and_insert(
    payload: ScrapeRequest,
    x_shortcuts_key: str = Header(default="", alias="X-Shortcuts-Key"),
):
    require_shortcuts_key(x_shortcuts_key)
    ensure_airtable_env()

    media_mode = (payload.mediaMode or "Images").lower()
    target = payload.targetCount

    all_rows = []
    for provider_name in payload.providers:
        provider = PROVIDER_MAP.get(provider_name)
        if not provider:
            # Soft-skip unknown provider rather than 500
            logging.warning(f"Unknown provider: {provider_name}; skipping")
            continue

        _, search_fn, normalize_fn = provider

        try:
            items = await search_fn(payload.topic, media_mode, target)
        except Exception as e:
            logging.exception(f"{provider_name} search failed")
            # Skip this provider on failure, continue with others
            continue

        for raw in items:
            row = normalize_fn(raw)

            # Dedup by source URL in Airtable
            try:
                exists = await find_by_source_url(row["url"])
            except Exception:
                exists = False

            if not exists:
                try:
                    await insert_record(row, meta={
                        "run_id": payload.runId or "",
                        "provider": provider_name,
                    })
                except Exception:
                    logging.exception("Airtable insert failed")

            all_rows.append(row)

    return {"insertedOrSkipped": len(all_rows), "providersProcessed": payload.providers}
