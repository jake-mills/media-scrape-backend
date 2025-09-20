# app.py
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import asyncio

# Import your providers
from providers.openverse import fetch_openverse_async
# from providers.youtube import fetch_youtube_async
# from providers.archive import fetch_archive_async
# ...add others here...

app = FastAPI()

# ---- Health Endpoints ----
@app.get("/health")
async def health_get():
    return {"ok": True}

@app.head("/health")
async def health_head():
    # Return empty body but 200 so HEAD pings are happy
    return Response(status_code=200)

# ---- Config ----
SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()

# ---- Request/Response Models ----
class ScrapeRequest(BaseModel):
    topic: str
    searchDates: Optional[str] = None
    targetCount: int = 1
    providers: List[str]
    mediaMode: str
    runId: Optional[str] = None

class ScrapeResponse(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: str
    insertedCount: int
    inserted: List[Dict[str, Any]]

# ---- Main Route ----
@app.post("/scrape-and-insert", response_model=ScrapeResponse)
async def scrape_and_insert(
    payload: ScrapeRequest,
    x_shortcuts_key: str = Header(..., alias="X-Shortcuts-Key")
):
    # --- Key Check ---
    if SHORTCUTS_KEY and x_shortcuts_key != SHORTCUTS_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Shortcuts-Key")

    run_id = payload.runId or "manual-run"
    inserted_items: List[Dict[str, Any]] = []

    # Loop over providers
    for provider in payload.providers:
        provider = provider.lower()
        items: List[Dict[str, Any]] = []

        try:
            if provider == "openverse":
                items = await fetch_openverse_async(
                    topic=payload.topic,
                    target_count=payload.targetCount,
                    search_dates=payload.searchDates,
                    use_precision=None,
                )

            # elif provider == "youtube":
            #     items = await fetch_youtube_async(...)
            #
            # elif provider == "archive":
            #     items = await fetch_archive_async(...)
            #
            # ... add more providers here ...

        except Exception as e:
            # Safety tweak: log the provider failure, but continue gracefully
            print(f"[WARN] Provider {provider} failed: {e}")
            items = []

        # Append normalized items
        if items:
            for idx, item in enumerate(items, start=1):
                # Add Run ID and index for Airtable traceability
                item["Run ID"] = run_id
                item["Index"] = idx
                inserted_items.append(item)

    response = ScrapeResponse(
        runId=run_id,
        requestedTarget=payload.targetCount,
        providers=payload.providers,
        mediaMode=payload.mediaMode,
        insertedCount=len(inserted_items),
        inserted=inserted_items,
    )
    return response
