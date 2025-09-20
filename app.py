# app.py
from __future__ import annotations

import os
import uuid
import logging
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# ---- Load env & logging -----------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("media-scrape-backend")

SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
if not SHORTCUTS_KEY:
    log.warning("SHORTCUTS_KEY is not set; all requests will be rejected (403).")

# ---- Optional imports: providers & Airtable helpers -------------------------
# Providers must expose async functions with the signature shown below.
# - fetch_youtube_async(topic: str, target_count: int, run_id: str, **kw) -> List[Dict]
# - fetch_openverse_async(topic: str, target_count: int, run_id: str, **kw) -> List[Dict]
try:
    from providers.youtube import fetch_youtube_async  # type: ignore
except Exception as e:
    fetch_youtube_async = None  # type: ignore
    log.warning("YouTube provider unavailable: %s", e)

try:
    from providers.openverse import fetch_openverse_async  # type: ignore
except Exception as e:
    fetch_openverse_async = None  # type: ignore
    log.warning("Openverse provider unavailable: %s", e)

# Airtable utilities expected by the pipeline
# - airtable_exists_by_source_url(url: str) -> bool
# - airtable_batch_create(rows: List[Dict[str, Any]]) -> int
try:
    from airtable import airtable_exists_by_source_url, airtable_batch_create  # type: ignore
except Exception as e:
    airtable_exists_by_source_url = None  # type: ignore
    airtable_batch_create = None  # type: ignore
    log.warning("Airtable helpers unavailable: %s", e)

# Optional utilities
try:
    from normalization import normalize_url  # type: ignore
except Exception:
    def normalize_url(u: str) -> str:
        return (u or "").strip()

# ---- FastAPI app ------------------------------------------------------------
app = FastAPI(
    title="Media Scrape Backend",
    version="1.3.0",
    description="Scrape images/videos from providers and insert into Airtable.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Root & health: silence 405s and give quick OKs -------------------------
@app.get("/")
async def root_ok():
    return {"ok": True, "service": "media-scrape-backend"}

@app.head("/")
async def root_head():
    # empty body, 200 OK
    return Response(status_code=200)

@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    return Response(status_code=200)

# ---- Models -----------------------------------------------------------------
MediaMode = Literal["Videos", "Images", "Both"]

class ScrapeRequest(BaseModel):
    topic: str = Field(..., description="Search topic/keywords.")
    searchDates: Optional[str] = Field(
        None,
        description="Optional date range string like '2000-2010' (provider support varies).",
    )
    targetCount: int = Field(1, ge=1, le=50, description="Desired number of items.")
    providers: List[str] = Field(..., description="Subset of ['YouTube','Openverse'].")
    mediaMode: MediaMode
    runId: Optional[str] = Field(default=None, description="Optional run identifier.")

class ScrapeResult(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    inserted: List[Dict[str, Any]]

# ---- Provider registry -------------------------------------------------------
# Map canonical provider name -> callable
PROVIDERS: Dict[str, Any] = {}
if fetch_youtube_async:
    PROVIDERS["YouTube"] = fetch_youtube_async
if fetch_openverse_async:
    PROVIDERS["Openverse"] = fetch_openverse_async

# ---- Helpers ----------------------------------------------------------------
def require_shortcuts_key(header_val: Optional[str]) -> None:
    if not SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Service not configured (SHORTCUTS_KEY).")
    if not header_val or header_val.strip() != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Forbidden (bad X-Shortcuts-Key).")

def want_provider(name: str, mode: MediaMode) -> bool:
    if mode == "Videos":
        return name == "YouTube"
    if mode == "Images":
        return name == "Openverse"
    # Both
    return name in ("YouTube", "Openverse")

def _summarize_for_response(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep the response body compact but useful.
    """
    out: List[Dict[str, Any]] = []
    for r in rows[:10]:
        out.append(
            {
                "Title": r.get("Title", ""),
                "Provider": r.get("Provider", ""),
                "Source_URL": r.get("Source URL", ""),
            }
        )
    return out

# ---- Main endpoint -----------------------------------------------------------
@app.post("/scrape-and-insert", response_model=ScrapeResult)
async def scrape_and_insert(
    payload: ScrapeRequest,
    request: Request,
    x_shortcuts_key: Optional[str] = Header(default=None, convert_underscores=False, alias="X-Shortcuts-Key"),
):
    # Auth
    require_shortcuts_key(x_shortcuts_key)

    run_id = payload.runId or str(uuid.uuid4())
    topic = payload.topic.strip()
    target = int(payload.targetCount)
    mode: MediaMode = payload.mediaMode
    requested_providers = [p.strip() for p in payload.providers or []]

    # Decide which providers to call based on mediaMode & user list overlap
    active: List[str] = []
    for name in requested_providers:
        if name in PROVIDERS and want_provider(name, mode):
            active.append(name)

    if not active:
        # Fallback: default to the provider implied by mediaMode
        if mode in ("Both", "Videos") and "YouTube" in PROVIDERS:
            active.append("YouTube")
        if mode in ("Both", "Images") and "Openverse" in PROVIDERS:
            active.append("Openverse")

    if not active:
        raise HTTPException(status_code=422, detail="No usable providers for this request.")

    # Run providers sequentially (keeps logs simpler, avoids provider rate limits)
    fetched: List[Dict[str, Any]] = []
    for name in active:
        fn = PROVIDERS.get(name)
        if not fn:
            log.warning("Provider %s missing, skipping.", name)
            continue

        try:
            results: List[Dict[str, Any]] = await fn(
                topic=topic,
                target_count=target,
                run_id=run_id,
                # Pass optional hints; providers may ignore unsupported ones
                search_dates=payload.searchDates,
            )
            log.info("Provider %s returned %d item(s).", name, len(results))
            fetched.extend(results)
        except TypeError as te:
            # Signature mismatch shows up as unexpected/missing kwarg in your logs;
            # log and continue so a single provider doesn't kill the run.
            log.warning("Provider %s failed: %s", name, te)
        except Exception as e:
            log.warning("Provider %s unexpected error: %s", name, e)

    if not fetched:
        return ScrapeResult(
            runId=run_id,
            requestedTarget=target,
            providers=active,
            mediaMode=mode,
            insertedCount=0,
            inserted=[],
        )

    # De-dup against Airtable and batch insert
    to_insert: List[Dict[str, Any]] = []
    if not airtable_exists_by_source_url or not airtable_batch_create:
        log.warning("Airtable helpers not loaded; returning fetched without insert.")
        return ScrapeResult(
            runId=run_id,
            requestedTarget=target,
            providers=active,
            mediaMode=mode,
            insertedCount=0,
            inserted=_summarize_for_response(fetched),
        )

    for row in fetched:
        src = normalize_url(row.get("Source URL", ""))
        if not src:
            continue
        try:
            exists = airtable_exists_by_source_url(src)
        except Exception as e:
            log.warning("Airtable exists check failed for %s: %s", src, e)
            exists = False

        if not exists:
            to_insert.append(row)

    inserted_count = 0
    if to_insert:
        try:
            inserted_count = airtable_batch_create(to_insert)
        except Exception as e:
            log.warning("Airtable batch insert failed: %s", e)

    return ScrapeResult(
        runId=run_id,
        requestedTarget=target,
        providers=active,
        mediaMode=mode,
        insertedCount=inserted_count,
        inserted=_summarize_for_response(to_insert[:inserted_count] or fetched[:1]),
    )
