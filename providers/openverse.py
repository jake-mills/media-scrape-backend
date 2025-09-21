# providers/openverse.py
"""
Openverse provider (Images + Videos)
Returns rich, normalized dicts with fields that map 1:1 to your Airtable schema.
"""

from __future__ import annotations

import httpx
from typing import Literal, Dict, Any, List

OPENVERSE_BASE = "https://api.openverse.engineering/v1"

def _endpoint(media: Literal["image", "video"]) -> str:
    return "images" if media == "image" else "videos"


async def fetch_openverse(
    *,
    topic: str,
    license_type: str = "commercial",
    page_size: int = 10,
    media: Literal["image", "video"] = "image",
) -> List[Dict[str, Any]]:
    url = f"{OPENVERSE_BASE}/{_endpoint(media)}/"
    params = {
        "q": topic,
        "license_type": license_type,
        "page_size": max(1, min(int(page_size), 50)),
    }
    headers = {"User-Agent": "media-scrape-backend/1.0"}

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        r = await client.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()

    results = data.get("results") or []
    out: List[Dict[str, Any]] = []

    for row in results:
        title = (row.get("title") or "").strip() or topic
        source_url = row.get("url") or row.get("foreign_landing_url")
        if not source_url:
            continue

        thumb = row.get("thumbnail") or row.get("url") or ""
        license_code = row.get("license") or ""
        created_on = row.get("created_on") or ""

        out.append(
            {
                "Media Type": "Images" if media == "image" else "Videos",
                "Provider": "Openverse",
                "Thumbnail": thumb,
                "Title": title,
                "Source URL": source_url,
                "Published/Created": created_on,
                "Copyright": license_code,
            }
        )

    return out
