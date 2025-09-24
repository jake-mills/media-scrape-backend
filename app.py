import os
from typing import Any, Dict, List

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from airtable_client import insert_row, find_by_source_url  # your updated names
from providers import openverse  # module with your Openverse code

APP_VERSION = "1.0.0"

REQUIRED_ENV = [
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
    "AIRTABLE_TABLE_NAME",
    "SHORTCUTS_KEY",
]

for k in REQUIRED_ENV:
    if not os.getenv(k):
        raise RuntimeError(f"Missing required env var: {k}")

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
SHORTCUTS_KEY = os.environ["SHORTCUTS_KEY"]
DEBUG_OPENVERSE = os.getenv("DEBUG_OPENVERSE") == "1"

app = FastAPI(title="media-scrape-backend", version=APP_VERSION)


# ---------- middleware: require Shortcuts key ----------
@app.middleware("http")
async def require_shortcuts_key(request: Request, call_next):
    path = request.url.path
    # Allow unauth'd endpoints
    if path in ("/", "/health", "/openapi.json"):
        return await call_next(request)

    sent = request.headers.get("X-Shortcuts-Key")
    if not sent or sent != SHORTCUTS_KEY:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid X-Shortcuts-Key"},
        )
    return await call_next(request)


# ---------- helpers ----------
def normalize_item(x: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single provider result into a dict with at least:
      - source_url: str (unique per asset)
      - title: str | None
      - thumbnail_url: str | None
    Extra fields will be passed through.
    """
    out = dict(x) if isinstance(x, dict) else {}

    # best-effort field mapping
    if "source_url" not in out:
        for cand in ("url", "href", "permalink", "link"):
            if cand in out:
                out["source_url"] = out[cand]
                break

    if "title" not in out:
        for cand in ("title", "name"):
            if cand in out:
                out["title"] = out[cand]
                break

    if "thumbnail_url" not in out:
        for cand in ("thumbnail", "thumbnail_url", "thumb", "preview"):
            if cand in out:
                out["thumbnail_url"] = out[cand]
                break

    return out


async def openverse_search(topic: str, media_mode: str, count: int) -> List[Dict[str, Any]]:
    """
    Call into providers.openverse using whatever function it actually has.
    We try, in order:
      - async def search(topic, media_mode, target_count)
      - async def search_images(query, limit)
      - def    search_images(query, limit)
      - async def query(query, limit)
      - def    query(query, limit)
    The results are normalized into a list[dict].
    """
    # Prefer the exact API if present
    if hasattr(openverse, "search"):
        res = openverse.search(topic, media_mode, count)
        if hasattr(res, "__await__"):  # async
            res = await res
    elif hasattr(openverse, "search_images"):
        func = openverse.search_images
        res = func(topic, count)
        if hasattr(res, "__await__"):
            res = await res
    elif hasattr(openverse, "query"):
        func = openverse.query
        # many libs expect `query` + `limit`
        try:
            res = func(topic, count)
        except TypeError:
            res = func(topic)
        if hasattr(res, "__await__"):
            res = await res
    else:
        raise RuntimeError(
            "providers.openverse does not expose search/search_images/query. "
            "Please implement one of those or adapt this shim."
        )

    # Res can be list[dict] or an object with .results
    if isinstance(res, dict) and "results" in res:
        items = res["results"]
    else:
        items = res

    if not isinstance(items, list):
        raise RuntimeError("Openverse provider returned an unexpected payload type.")

    return [normalize_item(x) for x in items][:count]


# ---------- routes ----------
@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION}


@app.post("/scrape-and-insert")
async def scrape_and_insert(payload: Dict[str, Any]):
    """
    Expected JSON (examples):
    {
      "topic": "wildlife",
      "targetCount": 3,
      "providers": ["Openverse"],
      "mediaMode": "Images",
      "runId": "optional-id",
      "searchTopics": "wildlife",
      "searchDates": "today"
    }
    Only topic/targetCount/mediaMode are used here; the rest pass-through.
    """
    topic = str(payload.get("topic") or payload.get("query") or "").strip()
    target_count = int(payload.get("targetCount") or payload.get("max_results") or 1)
    media_mode = str(payload.get("mediaMode") or "Images")

    if not topic:
        raise HTTPException(status_code=400, detail="Missing topic")

    # 1) fetch items
    items = await openverse_search(topic=topic, media_mode=media_mode, count=target_count)

    # 2) insert new items into Airtable (skip if source_url already exists)
    inserted = 0
    for it in items:
        src = it.get("source_url")
        if not src:
            continue
        try:
            existing = find_by_source_url(src)
        except Exception:
            existing = None

        if existing:
            continue

        # Build row for your Airtable schema
        row = {
            "Title": it.get("title") or topic,
            "Source URL": src,
            "Thumbnail": it.get("thumbnail_url"),
            "Topic": topic,
            "Provider": "Openverse",
            "Mode": media_mode,
        }

        try:
            insert_row(row)
            inserted += 1
        except Exception as e:
            # don’t fail whole request for one bad row
            if DEBUG_OPENVERSE:
                print(f"[airtable] insert failed for {src}: {e}")

    return {"inserted": inserted, "fetched": len(items), "topic": topic}
