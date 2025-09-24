# app.py
import os
import hmac
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from airtable_client import insert_record, find_by_source_url
from providers import openverse

app = FastAPI(title="Media Scrape Backend", version="1.0.0")

# --- Auth setup ---
EXPECTED_KEY = (os.getenv("SHORTCUTS_KEY") or "").strip()

def _get_shortcuts_key(req: Request) -> str:
    h = req.headers
    return (h.get("x-shortcuts-key") or h.get("X-Shortcuts-Key") or h.get("shortcuts-key") or "").strip()

def _is_key_ok(got: str) -> bool:
    return hmac.compare_digest(got, EXPECTED_KEY)

@app.middleware("http")
async def require_shortcuts_key(request: Request, call_next):
    # Allow health + debug without key
    if request.url.path not in ("/health", "/__debug_key", "/openapi.json", "/", "/docs", "/redoc"):
        got = _get_shortcuts_key(request)
        if not EXPECTED_KEY:
            return JSONResponse({"detail": "Server missing SHORTCUTS_KEY"}, status_code=500)
        if not _is_key_ok(got):
            return JSONResponse({"detail": "Invalid X-Shortcuts-Key"}, status_code=401)
    return await call_next(request)

# --- Health + Debug ---
@app.get("/health")
async def health():
    return {"status": "ok", "version": app.version}

@app.get("/__debug_key")
async def debug_key(request: Request):
    got = _get_shortcuts_key(request)
    def mask(s: str) -> str:
        if not s: return ""
        if len(s) <= 6: return "*" * len(s)
        return s[:3] + "…" + s[-3:]
    return {
        "env_present": bool(EXPECTED_KEY),
        "env_masked": mask(EXPECTED_KEY),
        "header_present": bool(got),
        "header_masked": mask(got),
        "equal": _is_key_ok(got),
    }

# --- Scrape + Insert route ---
@app.post("/scrape-and-insert")
async def scrape_and_insert(payload: dict, request: Request):
    topic = payload.get("topic")
    target_count = payload.get("targetCount", 1)
    providers = payload.get("providers", ["Openverse"])
    media_mode = payload.get("mediaMode", "Images")
    run_id = payload.get("runId", "manual")

    results = []
    if "Openverse" in providers:
        items = await openverse.search(topic, media_mode, target_count)
        for idx, item in enumerate(items, start=1):
            # de-dupe by Source URL
            existing = find_by_source_url(item["url"])
            if existing:
                continue

            record = {
                "Index": idx,
                "Media Type": media_mode,
                "Provider": "Openverse",
                "Thumbnail": item.get("thumbnail"),
                "Title": item.get("title"),
                "Source URL": item.get("url"),
                "Search Topics Used": topic,
                "Search Dates Used": payload.get("searchDates", ""),
                "Published/Created": item.get("published"),
                "Copyright": item.get("license"),
                "Run ID": run_id,
                "Notes": "",
            }
            insert_record(record)
            results.append(record)

    return {"inserted": len(results), "results": results}
