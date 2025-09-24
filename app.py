import os
import logging
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from providers.openverse import search as openverse_search
from airtable_client import insert_record, find_by_source_url

APP_VERSION = "1.0.0"

SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "").strip()

app = FastAPI(title="media-scrape-backend", version=APP_VERSION)
logging.basicConfig(level=logging.INFO)

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Search query")
    targetCount: int = Field(1, ge=1, le=50)
    providers: List[str] = Field(default_factory=lambda: ["Openverse"])
    mediaMode: str = Field("Images")
    runId: Optional[str] = None
    searchTopics: Optional[str] = None
    searchDates: Optional[str] = None

def require_shortcuts_key(key_header: str):
    if not SHORTCUTS_KEY:
        raise HTTPException(status_code=500, detail="Server missing SHORTCUTS_KEY")
    if key_header != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Shortcuts-Key")

def ensure_airtable_env():
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
        raise HTTPException(status_code=500, detail="Airtable env missing")

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

    media_mode = (payload.mediaMode or "Images").strip().lower()
    target = payload.targetCount
    topic = payload.topic.strip()

    normalized_mode = "Image" if media_mode.startswith("image") else "Video"

    all_rows: List[Dict[str, Any]] = []
    for provider_name in payload.providers:
        try:
            if provider_name == "Openverse":
                items = await openverse_search(topic, media_mode, target)
            else:
                logging.warning(f"Unknown provider: {provider_name}; skipping")
                continue
        except Exception:
            logging.exception(f"{provider_name} search failed")
            continue

        for it in items:
            url = it.get("url") or ""
            if not url:
                continue

            exists = False
            try:
                exists = await find_by_source_url(url)
            except Exception:
                logging.exception("Airtable find_by_source_url failed")

            if not exists:
                fields = {
                    "Media Type": normalized_mode,
                    "Provider": provider_name,
                    "Thumbnail": it.get("thumbnail") or "",
                    "Title": it.get("title") or "",
                    "Source URL": url,
                    "Search Topics Used": payload.searchTopics or topic,
                    "Search Dates Used": payload.searchDates or "",
                    "Published/Created": it.get("published") or "",
                    "Copyright": it.get("copyright") or "",
                    "Run ID": payload.runId or "",
                    "Notes": it.get("notes") or "",
                }
                try:
                    await insert_record(fields)
                except Exception:
                    logging.exception("Airtable insert failed")

            all_rows.append({
                "provider": provider_name,
                "title": it.get("title") or "",
                "url": url,
            })

    return {"processed": len(all_rows), "providers": payload.providers}
