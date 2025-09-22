# providers/openverse.py
"""
Openverse provider (Images + Videos). No API key required.
If OPENVERSE_API_KEY is present, it will be sent as Authorization: Bearer <key>.
Returns list of dicts matching your Airtable columns.
"""
from __future__ import annotations

import os
from typing import Literal, Dict, Any, List, Optional
import httpx

OPENVERSE_BASE = "https://api.openverse.engineering/v1"

def _endpoint(media: Literal["image","video"]) -> str:
    return "images" if media == "image" else "videos"

async def search_openverse(*, topic: str, media: Literal["image","video"], limit: int, debug: bool=False) -> List[Dict[str, Any]]:
    params = {
        "q": topic,
        "page_size": min(max(limit, 1), 60),
        "license_type": "commercial",
    }
    headers = {"Accept": "application/json"}
    api_key = os.getenv("OPENVERSE_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    url = f"{OPENVERSE_BASE}/{_endpoint(media)}"

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()

    results = data.get("results", []) if isinstance(data, dict) else []
    out: List[Dict[str, Any]] = []
    for item in results:
        title = item.get("title") or ""
        source_url = item.get("url") or ""
        thumb = item.get("thumbnail") or item.get("thumbnail_url") or ""
        created_on = item.get("created_on") or item.get("created") or ""
        license_code = item.get("license") or ""
        creator = item.get("creator") or ""
        # Build friendly copyright string
        copyright_str = ""
        if license_code and creator:
            copyright_str = f"{license_code.upper()} — {creator}"
        elif license_code:
            copyright_str = license_code.upper()

        out.append({
            "Media Type": "Images" if media == "image" else "Videos",
            "Provider": "Openverse",
            "Thumbnail": thumb,
            "Title": title,
            "Source URL": source_url,
            "Published/Created": created_on,
            "Copyright": copyright_str,
            # Leave these blank by default; app.py may fill Search/Run columns.
            "Notes": "",
        })

    if debug:
        print(f"[Openverse] topic={topic} media={media} fetched={len(out)}")

    return out
