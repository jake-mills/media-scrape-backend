# app.py
from __future__ import annotations

import os
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import httpx

from airtable_client import insert_row, find_by_source_url
from providers.openverse import search_openverse

APP_VERSION = "1.0.0"

REQUIRED_ENV = ["AIRTABLE_API_KEY", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_NAME", "SHORTCUTS_KEY"]
for k in REQUIRED_ENV:
    if not os.getenv(k):
        raise RuntimeError(f"Missing required environment variable: {k}")

app = FastAPI(title="Media Scrape Backend", version=APP_VERSION)

class ScrapeRequest(BaseModel):
    topic: str
    targetCount: int
    providers: List[str]
    mediaMode: Literal["Images", "Videos"]
    runId: str
    # Optional echo-through to Airtable columns if provided
    searchTopicsUsed: Optional[str] = None
    searchDatesUsed: Optional[str] = None

class InsertedItem(BaseModel):
    title: Optional[str] = None
    provider: Optional[str] = None
    source_url: Optional[str] = None
    thumbnailURL: Optional[str] = None

class InsertEcho(BaseModel):
    runId: str
    requestedTarget: int
    providers: List[str]
    mediaMode: Literal["Images", "Videos"]
    insertedCount: int
    skippedCount: int
    inserted: List[Dict[str, Any]]

@app.get("/health")
def health():
    return {"status": "ok", "version": APP_VERSION}

@app.post("/scrape-and-insert", response_model=InsertEcho)
async def scrape_and_insert(payload: ScrapeRequest, x_shortcuts_key: str = Header(alias="X-Shortcuts-Key")):
    if x_shortcuts_key != os.getenv("SHORTCUTS_KEY"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    media = "image" if payload.mediaMode.lower().startswith("image") else "video"
    target = max(0, int(payload.targetCount))

    # Collect candidates from providers (Openverse for now)
    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    for provider in payload.providers:
        prov = provider.lower().strip()
        try:
            if prov == "openverse":
                items = await search_openverse(
                    topic=payload.topic,
                    media=media,
                    limit=target,
                    debug=bool(int(os.getenv("DEBUG_OPENVERSE", "0")))
                )
                results.extend(items)
            else:
                errors.append(f"Provider {provider} not yet implemented")
        except httpx.HTTPError as e:
            errors.append(f"Provider {provider} failed: {str(e)}")

    # De-dup by Source URL and enforce target
    seen = set()
    unique: List[Dict[str, Any]] = []
    for r in results:
        url = r.get("Source URL")
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(r)
        if len(unique) >= target:
            break

    inserted_count = 0
    skipped_count = 0
    inserted_rows: List[Dict[str, Any]] = []

    # add run + optional search metadata
    for idx, row in enumerate(unique, start=1):
        row["Run ID"] = payload.runId
        if payload.searchTopicsUsed:
            row["Search Topics Used"] = payload.searchTopicsUsed
        if payload.searchDatesUsed:
            row["Search Dates Used"] = payload.searchDatesUsed

        existing = find_by_source_url(row.get("Source URL"))
        if existing:
            skipped_count += 1
            continue

        rec = insert_row(row)
        if rec:
            inserted_count += 1
            inserted_rows.append({
                "title": row.get("Title"),
                "provider": row.get("Provider"),
                "source_url": row.get("Source URL"),
                "thumbnailURL": row.get("Thumbnail")
            })
        else:
            errors.append("Unknown Airtable insert failure")

    if errors and inserted_count == 0:
        raise HTTPException(status_code=502, detail="; ".join(errors))

    return InsertEcho(
        runId=payload.runId,
        requestedTarget=payload.targetCount,
        providers=payload.providers,
        mediaMode=payload.mediaMode,
        insertedCount=inserted_count,
        skippedCount=skipped_count,
        inserted=inserted_rows,
    )
