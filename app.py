# app.py
import os
import asyncio
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

# --- Internal helpers (these should already exist in your repo) ---
from normalization import normalize_url as normalize_url_util
from date_utils import parse_search_dates
from airtable import (
    airtable_exists_by_source_url,
    airtable_batch_create,
)
from providers.youtube import fetch_youtube_async
from providers.openverse import fetch_openverse_async

# ---------- FastAPI app ----------
app = FastAPI(title="Media Scrape Backend")

# ---------- Health ----------
@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    # empty body, just a 200 so monitors/keepalives succeed
    return Response(status_code=200)

# ---------- Auth (X-Shortcuts-Key) ----------
SHORTCUTS_KEY = os.environ.get("SHORTCUTS_API_KEY", "") or os.environ.get("SHORTCUTS_KEY", "")

@app.middleware("http")
async def require_shortcuts_key(request: Request, call_next):
    # allow public docs + health
    if request.url.path in ("/health", "/docs", "/openapi.json", "/redoc"):
        return await call_next(request)

    header_key = request.headers.get("x-shortcuts-key")
    if not SHORTCUTS_KEY or header_key != SHORTCUTS_KEY:
        return Response(
            content="Forbidden: missing or invalid X-Shortcuts-Key",
            status_code=status.HTTP_403_FORBIDDEN,
        )
    return await call_next(request)

# ---------- CORS (relaxed; tighten in prod if desired) ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Env for Airtable ----------
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", "")

# ---------- Utilities ----------
def to_airtable_fields(item: Dict[str, Any], run_id: str, topic: str, search_dates_str: str) -> Dict[str, Any]:
    """
    Convert a normalized provider item into Airtable 'fields' dict
    matching your schema exactly.
    Expected normalized item keys (as returned by your provider functions):
      - "type"         -> "Video" | "Image"
      - "provider"     -> e.g. "YouTube" | "Openverse"
      - "title"        -> str
      - "url"          -> str (final source url)
      - "copyright"    -> str (or None)
      - "published"    -> ISO date (YYYY-MM-DD) or ''
      - "thumbnail"    -> {'url': 'https://...'}  (optional)
      - "notes"        -> str (optional)
    """
    fields: Dict[str, Any] = {
        "Media Type": item.get("type") or "",
        "Provider": item.get("provider") or "",
        "Title": item.get("title") or "",
        "Source URL": item.get("url") or "",
        "Search Topics Used": topic,
        "Search Dates Used": search_dates_str,
        "Published/Created": item.get("published") or "",
        "Copyright": item.get("copyright") or "",
        "Run ID": run_id,
        "Notes": item.get("notes") or "",
    }

    thumb = (item.get("thumbnail") or {}) if isinstance(item.get("thumbnail"), dict) else None
    thumb_url = thumb.get("url") if thumb else None
    if thumb_url:
        # Airtable attachments must be an array of {"url": "..."}
        fields["Thumbnail"] = [{"url": thumb_url}]
    else:
        # ensure the column exists but stays empty if no thumbnail
        # (optional – Airtable allows omission as well)
        pass

    return fields

async def choose_and_fetch(
    topic: str,
    search_dates_str: str,
    target_count: int,
    providers: List[str],
    media_mode: str,
) -> List[Dict[str, Any]]:
    """
    Call selected providers in parallel and return a list of normalized items.
    """
    tasks: List[asyncio.Task] = []

    # Videos
    if media_mode in ("Videos", "Both") and any(p.lower() == "youtube" for p in providers):
        tasks.append(asyncio.create_task(
            fetch_youtube_async(
                topic=topic,
                target_count=target_count,
                search_dates=search_dates_str,
            )
        ))

    # Images
    if media_mode in ("Images", "Both") and any(p.lower() == "openverse" for p in providers):
        tasks.append(asyncio.create_task(
            fetch_openverse_async(
                topic=topic,
                target_count=target_count,
                search_dates=search_dates_str,
                use_precision=False,  # tweak if you wire precision later
            )
        ))

    results: List[Dict[str, Any]] = []
    if not tasks:
        return results

    done = await asyncio.gather(*tasks, return_exceptions=True)
    for pack in done:
        if isinstance(pack, Exception):
            # swallow provider exceptions; keep other results
            # (alternatively, raise to fail the whole request)
            continue
        # each provider returns a list of normalized dicts
        results.extend(pack or [])

    return results

async def insert_into_airtable_unique(records: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Deduplicate by 'Source URL' against Airtable, then batch insert.
    Returns (inserted_count, inserted_items_list).
    """
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
        raise HTTPException(status_code=500, detail="Airtable environment variables are not configured.")

    to_create: List[Dict[str, Any]] = []
    inserted_items: List[Dict[str, Any]] = []

    for item in records:
        source_url = item.get("fields", {}).get("Source URL", "")
        if not source_url:
            continue

        normalized = normalize_url_util(source_url)
        exists = await airtable_exists_by_source_url(
            base_id=AIRTABLE_BASE_ID,
            table_name=AIRTABLE_TABLE_NAME,
            source_url=normalized,
        )
        if exists:
            continue

        to_create.append(item)
        inserted_items.append(item["fields"])

    # Airtable batch creates in chunks (10 or 25 depending on your helper)
    if to_create:
        await airtable_batch_create(
            base_id=AIRTABLE_BASE_ID,
            table_name=AIRTABLE_TABLE_NAME,
            records=to_create,
        )

    return len(inserted_items), inserted_items

# ---------- Main endpoint ----------
@app.post("/scrape-and-insert")
async def scrape_and_insert(payload: Dict[str, Any]):
    """
    Shortcut JSON body contract:
      {
        "topic": "wildlife",
        "searchDates": "2000-2010",
        "targetCount": 1,
        "providers": ["YouTube"] | ["Openverse"] | ["YouTube","Openverse"],
        "mediaMode": "Videos" | "Images" | "Both",
        "runId": "manual-test-001"
      }
    """
    # Validate minimum input
    topic: str = (payload.get("topic") or "").strip()
    search_dates_str: str = (payload.get("searchDates") or "").strip()
    target_count: int = int(payload.get("targetCount") or 1)
    providers: List[str] = payload.get("providers") or []
    media_mode: str = (payload.get("mediaMode") or "").strip() or "Both"
    run_id: str = (payload.get("runId") or "").strip() or "run-" + os.urandom(4).hex()

    if not topic:
        raise HTTPException(status_code=422, detail="Missing 'topic'")
    if not providers:
        raise HTTPException(status_code=422, detail="Missing 'providers' array")
    if media_mode not in ("Videos", "Images", "Both"):
        raise HTTPException(status_code=422, detail="Invalid 'mediaMode'")

    # Parse/normalize dates (your helper can accept ranges like "2000-2010")
    # If your helper returns a (start, end) tuple, still keep the original string for Airtable column.
    _ = parse_search_dates(search_dates_str)  # parsed range if you need it downstream

    # Fetch
    fetched: List[Dict[str, Any]] = await choose_and_fetch(
        topic=topic,
        search_dates_str=search_dates_str,
        target_count=target_count,
        providers=providers,
        media_mode=media_mode,
    )

    # Normalize to Airtable 'records' array
    records: List[Dict[str, Any]] = []
    for item in fetched:
        fields = to_airtable_fields(item, run_id=run_id, topic=topic, search_dates_str=search_dates_str)
        records.append({"fields": fields})

    # Deduplicate + insert
    inserted_count, inserted_items = await insert_into_airtable_unique(records)

    return {
        "runId": run_id,
        "requestedTarget": target_count,
        "providers": providers,
        "mediaMode": media_mode,
        "insertedCount": inserted_count,
        "inserted": inserted_items,
    }
