from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Dict
import os

# import your provider functions
# from providers.youtube import fetch_youtube_async
# from providers.openverse import fetch_openverse_async
# ... add others as needed ...

app = FastAPI()

# --- Health endpoints ---
@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    # Return empty body but 200 so HEAD pings succeed
    return Response(status_code=200)

# --- Middleware to enforce X-Shortcuts-Key ---
SHORTCUTS_KEY = os.environ.get("SHORTCUTS_KEY", "")

@app.middleware("http")
async def require_key(request: Request, call_next):
    if request.url.path in ("/health", "/docs", "/openapi.json"):
        # Don’t enforce auth on health/docs
        return await call_next(request)

    header_key = request.headers.get("x-shortcuts-key")
    if not SHORTCUTS_KEY or header_key != SHORTCUTS_KEY:
        return Response(
            content="Forbidden: missing or invalid X-Shortcuts-Key",
            status_code=status.HTTP_403_FORBIDDEN,
        )
    return await call_next(request)

# --- CORS (optional, safe defaults) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict in production if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Example scrape-and-insert endpoint ---
@app.post("/scrape-and-insert")
async def scrape_and_insert(payload: Dict[str, Any], request: Request):
    """
    Orchestrates provider fetches, dedupes, and inserts into Airtable.
    The shape of `payload` should match what your Shortcut sends:
    {
      "topic": str,
      "searchDates": str,
      "targetCount": int,
      "providers": [str],
      "mediaMode": str,
      "runId": str
    }
    """
    topic = payload.get("topic")
    search_dates = payload.get("searchDates")
    target_count = payload.get("targetCount", 1)
    providers = payload.get("providers", [])
    media_mode = payload.get("mediaMode")
    run_id = payload.get("runId")

    inserted = []  # this will hold normalized dicts to insert into Airtable

    # Example: call one provider (pseudo, fill in with your functions)
    # async with aiohttp.ClientSession() as session:
    #     if "YouTube" in providers and media_mode in ("Videos", "Both"):
    #         yt_results = await fetch_youtube_async(
    #             topic=topic,
    #             target_count=target_count,
    #             search_dates=search_dates,
    #             session=session,
    #         )
    #         inserted.extend(yt_results)
    #
    #     if "Openverse" in providers and media_mode in ("Images", "Both"):
    #         ov_results = await fetch_openverse_async(
    #             topic=topic,
    #             target_count=target_count,
    #             search_dates=search_dates,
    #             use_precision=False,
    #             session=session,
    #         )
    #         inserted.extend(ov_results)

    # TODO: push `inserted` records to Airtable here

    return {
        "runId": run_id,
        "requestedTarget": target_count,
        "providers": providers,
        "mediaMode": media_mode,
        "insertedCount": len(inserted),
        "inserted": inserted,
    }
