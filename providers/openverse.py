# providers/openverse.py
from __future__ import annotations

import os
from typing import Any, Dict, List

import httpx

OPENVERSE_BASE = "https://api.openverse.org/v1"
DEFAULT_PAGE_SIZE = 30

# Build a single client with required headers
def _client() -> httpx.Client:
    headers = {
        "User-Agent": "media-scrape-backend/1.0 (+https://example.com)",
    }
    api_key = os.getenv("OPENVERSE_API_KEY", "").strip()
    if api_key:
        # Openverse expects an API key header
        headers["X-Api-Key"] = api_key
    return httpx.Client(base_url=OPENVERSE_BASE, headers=headers, timeout=30.0)

def search_images(topic: str, limit: int = DEFAULT_PAGE_SIZE) -> List[Dict[str, Any]]:
    """
    Query Openverse Images for a topic. Returns normalized items that
    our app expects to write to Airtable.
    """
    params = {
        "q": topic,
        "page_size": min(limit, DEFAULT_PAGE_SIZE),
        # keep commercial-only like your tests; adjust if you want broader
        "license_type": "commercial",
    }

    with _client() as client:
        r = client.get("/images/", params=params)
        # If the API key is missing/invalid, make the reason obvious
        if r.status_code == 401:
            raise httpx.HTTPStatusError(
                "401 Unauthorized from Openverse (missing/invalid OPENVERSE_API_KEY)",
                request=r.request,
                response=r,
            )
        r.raise_for_status()
        data = r.json()

    results = []
    for idx, it in enumerate(data.get("results", []), start=1):
        # Map Openverse fields to our union “InsertEcho” shape that Airtable understands
        # NOTE: we keep your Airtable column names; mapping to them happens in airtable_client
        results.append(
            {
                "index": idx,
                "media_type": "Image",
                "provider": "Openverse",
                "title": it.get("title"),
                "source_url": it.get("url"),
                "thumbnail_url": it.get("thumbnail"),
                "copyright": it.get("license"),
                # extra info you may want later
                "width": it.get("width"),
                "height": it.get("height"),
                "tags": ", ".join(t.get("name", "") for t in (it.get("tags") or []) if t.get("name")),
                # raw for debugging
                "_raw": it,
            }
        )

    return results
