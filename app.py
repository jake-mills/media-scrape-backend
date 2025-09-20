# app.py (full)

import os
from typing import List, Literal, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import httpx

from airtable_client import AirtableClient
from providers.openverse import OpenverseProvider

APP = FastAPI(title="media-scrape-backend")

SHORTCUTS_KEY = os.getenv("SHORTCUTS_KEY", "").strip()
if not SHORTCUTS_KEY:
    print("WARNING SHORTCUTS_KEY is not set; all requests will be rejected (403).")

# ---------- Models ----------
MediaMode = Literal["Images", "Videos"]

class ScrapeRequest(BaseModel):
    topic: str
    searchDates: Optional[str] = None
    targetCount: int = 1
    providers: List[str]
    mediaMode: MediaMode
    runId: Optional[str] = None


class InsertedRow(BaseModel):
    Title: str
    Provider: str
    Source_URL: str


class ScrapeResponse(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: MediaMode
    insertedCount: int
    inserted: List[InsertedRow]


# ---------- Utils ----------
async def keepalive():
    # lightweight keepalive for Render (uses httpx, not aiohttp)
    # HEAD /health so we don't hit FastAPI routing errors
    url = "http://0.0.0.0:10000/health"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.head(url)
    except Exception:
        pass


@APP.get("/health")
async def health():
    return {"status": "ok"}


# ---------- Routes ----------
@APP.post("/scrape-and-insert", response_model=ScrapeResponse)
async def scrape_and_insert(
    payload: ScrapeRequest,
    x_shortcuts_key: str = Header(default="", alias="X-Shortcuts-Key"),
):
    # auth gate
    if not SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Service not configured (SHORTCUTS_KEY).")
    if x_shortcuts_key.strip() != SHORTCUTS_KEY:
        raise HTTPException(status_code=403, detail="Forbidden.")

    run_id = payload.runId or "swagger-test-openverse-OK"
    mode = payload.mediaMode

    # We currently only wired Openverse; ignore others for now
    provider_names = [p for p in payload.providers if p.lower() == "openverse"]

    # fetch from providers
    records: List[InsertedRow] = []

    if "openverse" in [p.lower() for p in provider_names] and mode == "Images":
        items = await OpenverseProvider.fetch_openverse_async(
            query=payload.topic,
            license_type="commercial",
            page_size=max(1, min(5, payload.targetCount)),
            run_id=run_id,
        )
        # insert into Airtable with de-dup by Source_URL
        client = AirtableClient()
        for it in items[: payload.targetCount]:
            created = await client.insert_if_new(
                title=it["title"],
                provider="Openverse",
                source_url=it["source_url"],
            )
            if created:
                records.append(
                    InsertedRow(
                        Title=it["title"] or "",
                        Provider="Openverse",
                        Source_URL=it["source_url"],
                    )
                )

    # craft response
    return ScrapeResponse(
        runId=run_id,
        requestedTarget=payload.targetCount,
        providers=provider_names or payload.providers,
        mediaMode=mode,
        insertedCount=len(records),
        inserted=records,
    )


# ---------- Root handling to silence 405s ----------
@APP.get("/")
async def root_index():
    # Prevent noisy GET / 405s in Render logs
    return {"ok": True}
