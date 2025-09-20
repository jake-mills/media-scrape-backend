from __future__ import annotations

import httpx
import logging
from typing import Literal

log = logging.getLogger("openverse")


def _endpoint_for_media(media: Literal["image", "video"]) -> str:
    return "images" if media == "image" else "videos"


async def fetch_openverse_async(
    *,
    query: str,
    media: Literal["image", "video"] = "image",
    license_type: str = "commercial",
    page_size: int = 1,
    debug: bool = False,
) -> list[dict]:
    """
    Calls Openverse and returns a list of {title, source_url, provider} dicts.
    """
    endpoint = _endpoint_for_media(media)
    url = f"https://api.openverse.engineering/v1/{endpoint}/"
    params = {"q": query, "license_type": license_type, "page_size": page_size}
    headers = {"User-Agent": "media-scrape-backend/1.0"}

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        r = await client.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()

    results = data.get("results", [])
    if debug:
        log.info("[Openverse] Returning %s item(s)", len(results))

    items: list[dict] = []
    for row in results:
        title = row.get("title") or query
        # Prefer the direct asset URL; fall back to landing page if needed
        src = row.get("url") or row.get("foreign_landing_url")
        if not src:
            continue
        items.append(
            {"title": title, "provider": "Openverse", "source_url": src}
        )
    return items
