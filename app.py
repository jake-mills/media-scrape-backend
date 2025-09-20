import os
import re
import logging
from typing import List, Literal

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from starlette.responses import JSONResponse, PlainTextResponse
import anyio

# Providers
from providers.openverse import fetch_openverse_async

# Airtable client (our local helper built on pyairtable)
from airtable_client import AirtableClient

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# App & middleware
# -----------------------------------------------------------------------------
app = FastAPI(title="Media Scrape Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "")
DEBUG_OPENVERSE = os.getenv("DEBUG_OPENVERSE") == "1"

airtable = AirtableClient.from_env()
if airtable is None:
    log.warning("Airtable client not configured. Inserts will be skipped.")

# -----------------------------------------------------------------------------
# Models (Pydantic v2)
# -----------------------------------------------------------------------------
class InsertedItem(BaseModel):
    Title: str
    Provider: str
    Source_URL: str


class ScrapeAndInsertRequest(BaseModel):
    topic: str
    searchDates: str  # "YYYY-YYYY"
    targetCount: int = 1
    providers: List[str]
    mediaMode: Literal["Images", "Videos"]
    runId: str | None = None

    @field_validator("searchDates")
    @classmethod
    def validate_dates(cls, v: str) -> str:
        if not re.fullmatch(r"\d{4}-\d{4}", v.strip()):
            raise ValueError("searchDates must be in the format 'YYYY-YYYY'")
        start, end = map(int, v.split("-"))
        if start > end:
            raise ValueError("searchDates start must be <= end")
        return v


class ScrapeAndInsertResponse(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: str
    insertedCount: int
    inserted: List[InsertedItem]


# -----------------------------------------------------------------------------
# Health & simple roots (also silence HEAD 405s)
# -----------------------------------------------------------------------------
@app.get("/health")
@app.head("/health")
def health() -> PlainTextResponse:
    return PlainTextResponse("OK")

@app.get("/")
def root() -> dict:
    return {"status": "ok"}

@app.head("/")
def head_root() -> PlainTextResponse:
    # Return 200 to quiet Render's HEAD checks at root
    return PlainTextResponse("")


# -----------------------------------------------------------------------------
# Main endpoint
# -----------------------------------------------------------------------------
@app.post("/scrape-and-insert", response_model=ScrapeAndInsertResponse)
async def scrape_and_insert(
    payload: ScrapeAndInsertRequest,
    shortcuts_key: str | None = Header(default=None, alias="X-Shortcuts-Key"),
):
    # Auth gate
    if not SHORTCUTS_KEY:
        log.warning("SHORTCUTS_KEY is not set; all requests will be rejected.")
        raise HTTPException(status_code=403, detail="Service not configured (SHORTCUTS_KEY).")
    if shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Forbidden (bad X-Shortcuts-Key).")

    run_id = payload.runId or "run-" + os.urandom(6).hex()
    requested_target = max(0, int(payload.targetCount))

    # Currently support only "Openverse"
    providers = [p for p in payload.providers if p.lower() == "openverse"]
    if not providers:
        return ScrapeAndInsertResponse(
            runId=run_id,
            requestedTarget=requested_target,
            providers=payload.providers,
            mediaMode=payload.mediaMode,
            insertedCount=0,
            inserted=[],
        )

    # Query provider
    media = "image" if payload.mediaMode == "Images" else "video"
    ov_items = await fetch_openverse_async(
        query=payload.topic,
        media=media,
        license_type="commercial",
        page_size=max(1, requested_target),
        debug=DEBUG_OPENVERSE,
    )

    inserted: list[InsertedItem] = []

    # Optionally insert into Airtable if configured
    async def _maybe_insert_one(item: dict) -> None:
        fields = {
            "Title": item["title"],
            "Provider": item["provider"],
            "Source_URL": item["source_url"],
        }
        if airtable is None:
            return
        exists = await anyio.to_thread.run_sync(
            airtable.exists_by_source_url, fields["Source_URL"]
        )
        if not exists:
            await anyio.to_thread.run_sync(airtable.create_record, fields)
            inserted.append(InsertedItem(**fields))

    # Only process up to requested_target items
    tasks = [_maybe_insert_one(it) for it in ov_items[:requested_target]]
    if tasks:
        await anyio.gather(*tasks)

    resp = ScrapeAndInsertResponse(
        runId=run_id,
        requestedTarget=requested_target,
        providers=providers,
        mediaMode=payload.mediaMode,
        insertedCount=len(inserted),
        inserted=inserted,
    )
    return JSONResponse(resp.model_dump())


# Local dev runner (Render ignores this)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
