"""
Openverse provider.

Implements an async `search(topic, media_mode, target_count)` API that
returns a list of normalized media items for the app to insert.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

import httpx

OPENVERSE_BASE = "https://api.openverse.engineering/v1"

# Map our media_mode to the Openverse collection
_COLLECTION_BY_MODE = {
    "Images": "images",
    "Image": "images",
    "Videos": "videos",
    "Video": "videos",
}


def _norm_image(item: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize an Openverse image record to the shape the app expects."""
    # Openverse image fields: id, title, url (landing), thumbnail, creator, source, license, etc.
    return {
        "provider": "Openverse",
        "mediaMode": "Images",
        "title": item.get("title") or item.get("id"),
        "source_url": item.get("url"),           # page on the creator/source site
        "media_url": item.get("thumbnail") or item.get("url"),  # something viewable
        "thumbnail_url": item.get("thumbnail") or item.get("url"),
        "author": item.get("creator"),
        "license": item.get("license"),
        "raw": item,  # keep full record for debugging
    }


def _norm_video(item: Dict[str, Any]) -> Dict[str, Any]:
    # Openverse video has similar keys; we still expose a usable media_url if present
    return {
        "provider": "Openverse",
        "mediaMode": "Videos",
        "title": item.get("title") or item.get("id"),
        "source_url": item.get("url"),
        "media_url": item.get("thumbnail") or item.get("url"),
        "thumbnail_url": item.get("thumbnail") or item.get("url"),
        "author": item.get("creator"),
        "license": item.get("license"),
        "raw": item,
    }


async def search(topic: str, media_mode: str, target_count: int) -> List[Dict[str, Any]]:
    """
    Search Openverse for `topic` and return up to `target_count` normalized items.

    Args
    ----
    topic: search query string
    media_mode: "Images" or "Videos" (case-insensitive accepted)
    target_count: max number of items to return (>=1)

    Returns
    -------
    List[dict] of normalized items
    """
    mode_key = (media_mode or "").strip().title()
    collection = _COLLECTION_BY_MODE.get(mode_key, "images")

    # page size 1..50 (Openverse caps at 50)
    page_size = max(1, min(50, int(target_count or 1)))

    headers = {}
    api_key = os.getenv("OPENVERSE_API_KEY") or os.getenv("OPENVERSE_KEY")
    if api_key:
        # Openverse currently supports optional API keys for higher quotas
        headers["Authorization"] = f"Bearer {api_key}"

    params = {
        "q": topic,
        "page_size": page_size,
        # Feel free to tweak filters if you want only CC-licensed, etc.
        # "license_type": "all",  # default
        # "license": "cc0,by",   # example
    }

    url = f"{OPENVERSE_BASE}/{collection}/"
    items: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=20.0) as client:
        # We’ll keep paging until we hit target_count or run out
        next_url: str | None = url
        next_params = params

        while next_url and len(items) < target_count:
            resp = await client.get(next_url, params=next_params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not isinstance(results, list):
                break

            for r in results:
                norm = _norm_image(r) if collection == "images" else _norm_video(r)
                items.append(norm)
                if len(items) >= target_count:
                    break

            # pagination
            next_url = data.get("next")
            next_params = None  # when "next" is an absolute URL, we don’t pass params again

    return items
