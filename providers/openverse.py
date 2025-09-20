# providers/openverse.py

from __future__ import annotations

import os
import aiohttp
from typing import Any, Dict, List, Optional

OPENVERSE_URL = "https://api.openverse.engineering/v1/images/"
DEBUG = os.getenv("DEBUG_OPENVERSE", "0") == "1"

def _log(msg: str) -> None:
    if DEBUG:
        print(f"[Openverse] {msg}", flush=True)

def _coerce_query(query: Optional[str], topic: Optional[str]) -> str:
    # Accept either key; prefer explicit query if present.
    q = (query or topic or "").strip()
    return q

async def fetch_openverse_async(
    *,
    # accept BOTH names so upstream callers can pass either
    query: Optional[str] = None,
    topic: Optional[str] = None,
    search_dates: Optional[str] = None,   # not used by API, but accepted for interface parity
    target_count: int = 1,
    run_id: str = "",                     # optional for logs
) -> List[Dict[str, Any]]:
    """
    Fetch images from Openverse. Returns a list of normalized dicts
    ready for Airtable insertion by the caller.
    """
    q = _coerce_query(query, topic)
    if not q:
        _log("No query/topic provided; returning empty list.")
        return []

    # Build request params
    page_size = max(1, min(20, int(target_count or 1)))
    params = {
        "q": q,
        "license_type": "commercial",  # keeps results broadly usable
        "page_size": page_size,
    }

    headers = {"User-Agent": "MediaScrape/1.0 (+render)"}
    timeout = aiohttp.ClientTimeout(total=15, connect=5)

    _log(f"run_id={run_id} q={q!r} params={params}")

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        try:
            async with session.get(OPENVERSE_URL, params=params) as resp:
                if resp.status != 200:
                    _log(f"HTTP {resp.status} from Openverse")
                    return []
                data = await resp.json()
        except Exception as e:
            _log(f"Request error: {e!r}")
            return []

    results = data.get("results", []) if isinstance(data, dict) else []
    items: List[Dict[str, Any]] = []

    for idx, rec in enumerate(results, start=1):
        # Normalize fields
        title = (rec.get("title") or "").strip()
        source_url = rec.get("url") or rec.get("foreign_landing_url") or ""
        license_code = rec.get("license") or ""
        created_on = rec.get("created_on") or ""
        thumb = rec.get("thumbnail") or rec.get("url") or ""

        items.append({
            "Index": idx,
            "Search Topics Used": q,
            "Thumbnail": {
                "id": rec.get("id") or "",
                "url": thumb,
                "filename": "",
            },
            "Title": title,
            "Source URL": source_url,
            "Copyright": license_code,
            "Media Type": "Image",
            "Provider": "Openverse",
            "Published": created_on,
            "Created": "",   # keep keys consistent with your table shape
        })

        if len(items) >= target_count:
            break

    _log(f"Returning {len(items)} item(s)")
    return items
