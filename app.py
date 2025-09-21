# app.py
import os
import logging
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---- IMPORTANT: pydantic v1 imports (compatible with pyairtable) ----
from pydantic import BaseModel, Field, validator

# Local modules
from airtable_client import (
    insert_row,
    find_by_source_url,
    AirtableError,   # custom error class raised by airtable_client on non-2xx
)

# Providers registry (currently only Openverse)
from providers.openverse import OpenverseProvider

# -----------------------------------------------------------------------------
# App + logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("media-scrape-backend")

app = FastAPI(title="Media Scrape Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock this down if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config / env
# -----------------------------------------------------------------------------
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
DEBUG_OPENVERSE = os.getenv("DEBUG_OPENVERSE", "0") in ("1", "true", "True")

if not SHORTCUTS_KEY:
    log.warning("Environment variable SHORTCUTS_KEY is not set. "
                "Requests to /scrape-and-insert will be rejected without it.")

# -----------------------------------------------------------------------------
# Pydantic models (v1)
# -----------------------------------------------------------------------------
MediaMode = Literal["Images", "Videos"]

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="What to search for")
    targetCount: int = Field(ge=1, le=100, description="How many items to try to insert")
    providers: List[Literal["Openverse"]] = Field(..., description="List of providers")
    mediaMode: MediaMode = Field(..., description="Images or Videos")
    searchDates: Optional[str] = Field(
        None, description="Optional date hint like '2000-2010'"
    )
    runId: Optional[str] = Field(None, description="Optional client run id / trace id")

    @validator("topic")
    def _topic_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic cannot be blank")
        return v.strip()

class InsertedItem(BaseModel):
    title: Optional[str] = None
    provider: Optional[str] = None
    source_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    creator: Optional[str] = None
    license: Optional[str] = None
    license_url: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    tags: Optional[List[str]] = None

class ScrapeResult(BaseModel):
    runId: Optional[str] = None
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    skippedCount: int
    inserted: List[InsertedItem]

# -----------------------------------------------------------------------------
# Error handling: return Airtable errors as JSON (not generic 500s)
# -----------------------------------------------------------------------------
@app.exception_handler(AirtableError)
async def airtable_error_handler(_, exc: AirtableError):
    """
    Converts AirtableError (raised in airtable_client) into a structured JSON response
    so you can immediately see the real Airtable validation/message.
    """
    return JSONResponse(
        status_code=exc.status or 500,
        content={
            "error": str(exc),
            "status": exc.status,
            "payload": getattr(exc, "payload", None),
            "airtable_response": getattr(exc, "response_json", None),
        },
    )

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

# -----------------------------------------------------------------------------
# Providers registry
# -----------------------------------------------------------------------------
PROVIDERS: Dict[str, Any] = {
    "Openverse": OpenverseProvider(debug=DEBUG_OPENVERSE),
    # If you add others later, register them here.
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def ensure_header_key(x_shortcuts_key: str) -> None:
    if not SHORTCUTS_KEY:
        raise HTTPException(
            status_code=500,
            detail="Server is missing SHORTCUTS_KEY env var.",
        )
    if x_shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Shortcuts-Key.")

# -----------------------------------------------------------------------------
# Main route
# -----------------------------------------------------------------------------
@app.post("/scrape-and-insert", response_model=ScrapeResult)
async def scrape_and_insert(
    body: ScrapeRequest,
    x_shortcuts_key: str = Header(..., alias="X-Shortcuts-Key"),
):
    """
    1) Auth via header
    2) Search via provider(s)
    3) De-dup by Source URL against Airtable
    4) Insert up to targetCount
    5) Return structured result
    """
    ensure_header_key(x_shortcuts_key)

    # Only currently supports Images for Openverse; Videos can be wired later
    if body.mediaMode == "Videos":
        raise HTTPException(status_code=400, detail="Videos not implemented for Openverse.")

    # Aggregate results from listed providers
    all_candidates: List[Dict[str, Any]] = []
    for name in body.providers:
        provider = PROVIDERS.get(name)
        if not provider:
            raise HTTPException(status_code=400, detail=f"Unknown provider: {name}")

        try:
            items = await provider.search(
                query=body.topic,
                license_type="commercial",  # tune as you wish
                page_size=max(10, body.targetCount),  # fetch enough; we’ll dedupe
                search_dates=body.searchDates,
            )
            log.info("Provider %s returned %d items", name, len(items))
            all_candidates.extend(items)
        except Exception as e:
            log.exception("Provider %s failed: %s", name, e)
            raise HTTPException(status_code=502, detail=f"Provider {name} failed: {e}")

    # De-dupe by source_url and insert up to targetCount
    inserted: List[InsertedItem] = []
    seen_urls = set()

    for item in all_candidates:
        if len(inserted) >= body.targetCount:
            break

        src = (item.get("source_url") or "").strip()
        if not src or src in seen_urls:
            continue
        seen_urls.add(src)

        # Skip if already in Airtable
        if find_by_source_url(src):
            continue

        # Perform the insert (airtable_client handles proper column names)
        # Expecting airtable_client.insert_row() to raise AirtableError with details on failure
        inserted_record = insert_row(item)
        inserted.append(InsertedItem(**inserted_record))

    result = ScrapeResult(
        runId=body.runId,
        requestedTarget=body.targetCount,
        providers=body.providers,
        mediaMode=body.mediaMode,
        insertedCount=len(inserted),
        skippedCount=max(0, len(all_candidates) - len(inserted)),
        inserted=inserted,
    )

    log.info(
        "Run %s complete: requested=%d inserted=%d skipped=%d",
        body.runId, body.targetCount, result.insertedCount, result.skippedCount
    )
    return result

# -----------------------------------------------------------------------------
# Local dev entry (ignored by Render/Gunicorn)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
