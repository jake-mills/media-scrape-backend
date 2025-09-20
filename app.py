import os, asyncio, uuid
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from providers.youtube import fetch_youtube_async
from providers.openverse import fetch_openverse_async
from providers.archive import fetch_archive_async
from normalization import normalize_url
from airtable import airtable_exists_by_source_url, airtable_batch_create
from date_utils import parse_search_dates, archive_year_bounds

load_dotenv()

# Defaults and safety checks
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "app7iv7XirA2VzppE")
if not AIRTABLE_API_KEY:
    raise RuntimeError("AIRTABLE_API_KEY missing. Set it in your environment.")
if not AIRTABLE_BASE_ID:
    raise RuntimeError("AIRTABLE_BASE_ID missing.")

SHORTCUTS_API_KEY = os.getenv("SHORTCUTS_API_KEY")

app = FastAPI(title="Media Scrape Backend", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScrapeSpec(BaseModel):
    topic: str
    searchDates: str = ""
    targetCount: int = Field(ge=1, le=100)
    providers: List[str] = Field(default_factory=list)
    runId: Optional[str] = None

@app.middleware("http")
async def require_key(request: Request, call_next):
    if request.url.path.startswith("/scrape-and-insert"):
        key = request.headers.get("X-Shortcuts-Key")
        if not key or key != SHORTCUTS_API_KEY:
            raise HTTPException(status_code=403, detail="Forbidden")
    return await call_next(request)

@app.get("/health")
async def health():
    return {"ok": True}

def map_to_airtable_fields(item: Dict[str, Any], index: int, run_id: str, topic: str, search_dates: str) -> Dict[str, Any]:
    fields = {
        "Index": index,
        "Media Type": item.get("type", ""),
        "Provider": item.get("provider", ""),
        "Title": item.get("title", ""),
        "Source URL": item.get("url", ""),
        "Search Topics Used": topic,
        "Search Dates Used": search_dates,
        "Published/Created": (item.get("published_at") or "")[:10],
        "Copyright": item.get("copyright", ""),
        "Run ID": run_id,
        "Notes": "",
    }
    thumb = item.get("thumbnail")
    if thumb:
        fields["Thumbnail"] = [{"url": thumb}]
    return fields

async def fetch_all(providers: List[str], topic: str, search_dates: str, wanted: int) -> List[Dict[str, Any]]:
    after, before = parse_search_dates(search_dates)
    arch_y1, arch_y2 = archive_year_bounds(search_dates)

    tasks = []
    for p in providers:
        key = (p or "").strip().lower()
        if key == "youtube":
            tasks.append(fetch_youtube_async(topic, max_results=max(50, wanted*2),
                                             published_after=after, published_before=before))
        elif key == "openverse":
            tasks.append(fetch_openverse_async(topic, page_size=max(30, wanted*2)))
        elif key == "archive":
            tasks.append(fetch_archive_async(topic, rows=max(50, wanted*2),
                                             year_start=arch_y1, year_end=arch_y2))
    out: List[Dict[str, Any]] = []
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                continue
            out.extend(r)
    return out

@app.post("/scrape-and-insert")
async def scrape_and_insert(spec: ScrapeSpec, x_shortcuts_key: str | None = Header(None)):
    topic = (spec.topic or "").strip()
    if not topic:
        raise HTTPException(status_code=400, detail="Missing topic")

    providers = spec.providers or []
    if not providers:
        raise HTTPException(status_code=400, detail="Missing providers")

    run_id = spec.runId or str(uuid.uuid4())
    wanted = min(max(spec.targetCount, 1), 100)

    candidates = await fetch_all(providers, topic, spec.searchDates, wanted)

    # Normalize & dedupe within batch
    seen = set()
    unique: List[Dict[str, Any]] = []
    for c in candidates:
        url = normalize_url(c.get("url") or "")
        c["url"] = url
        key = (c.get("provider",""), url)
        if url and key not in seen:
            seen.add(key)
            unique.append(c)

    # Check Airtable and collect up to 'wanted' new items
    new_items: List[Dict[str, Any]] = []
    for c in unique:
        if len(new_items) >= wanted:
            break
        src = c.get("url")
        if not src:
            continue
        exists = await airtable_exists_by_source_url(src)
        if exists:
            continue
        new_items.append(c)

    # Map to fields with Index + Run ID
    records_fields = []
    idx = 0
    for c in new_items[:wanted]:
        idx += 1
        records_fields.append(map_to_airtable_fields(c, index=idx, run_id=run_id, topic=spec.topic, search_dates=spec.searchDates))

    inserted = await airtable_batch_create(records_fields)
    return {
        "runId": run_id,
        "requestedTarget": wanted,
        "providers": providers,
        "insertedCount": len(inserted),
        "inserted": inserted
    }
