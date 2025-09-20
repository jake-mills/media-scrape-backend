# providers/openverse.py
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple
import httpx
import logging

logger = logging.getLogger("providers.openverse")

OPENVERSE_BASE = "https://api.openverse.engineering/v1/images/"

# Openverse returns fields incl.:
# id, title, url (full), thumbnail, foreign_landing_url (page), license, source, creator, tags, etc.

def _build_query(topic: str, target_count: int) -> Dict[str, Any]:
    # page_size max is typically 500, but we’ll stay conservative
    page_size = max(1, min(target_count, 50))
    return {
        "q": topic,
        "page_size": page_size,
        # you could add filters here like:
        # "license_type": "all",
        # "mature": "false",
    }

def _map_item_to_normalized(item: Dict[str, Any]) -> Dict[str, Any]:
    # Safely extract common fields
    title = item.get("title") or "Untitled"
    thumb = item.get("thumbnail") or item.get("url")  # fallback to full if no thumb
    full_url = item.get("url")
    landing = item.get("foreign_landing_url") or full_url
    provider = (item.get("source") or "Openverse").title()
    license_str = item.get("license") or "Openverse Standard"

    # We don’t have a clean published date; leave empty and let Airtable store blank.
    published = ""

    return {
        "Title": title,
        "Source URL": landing or full_url or "",
        "Thumbnail": {
            "url": thumb or "",
            "filename": "",
        },
        "Copyright": license_str,
        "Media Type": "Image",
        "Provider": provider,
        "Published/Created": published,
    }

async def _fetch_page(
    client: httpx.AsyncClient,
    params: Dict[str, Any],
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Returns (status_code, items)
    """
    try:
        resp = await client.get(OPENVERSE_BASE, params=params)
        status = resp.status_code
        if status == 200:
            data = resp.json()
            results = data.get("results") or []
            if not isinstance(results, list):
                logger.warning("Openverse: results not list; got %r", type(results))
                return status, []
            return status, results
        else:
            # Log the first 200 chars of any error body for visibility
            body = ""
            try:
                body = resp.text[:200]
            except Exception:
                pass
            logger.warning("Openverse non-200 (%s). URL=%s  Snip=%s", status, str(resp.request.url), body)
            return status, []
    except Exception as e:
        logger.exception("Openverse request exception: %s", e)
        return 0, []

async def fetch_openverse_async(
    topic: str,
    target_count: int,
    search_dates: Optional[str] = None,
    use_precision: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """
    Asynchronous fetcher for Openverse images.
    Returns a list of normalized dicts ready for Airtable mapping.
    """
    # Build params
    params = _build_query(topic, target_count)

    # Client with redirect following + sane timeouts
    timeout = httpx.Timeout(connect=10.0, read=20.0)
    headers = {
        "User-Agent": "MediaScrape/1.0 (+https://render.com) openverse-client",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,  # <— important for the 301/302 we observed
    ) as client:
        status, items = await _fetch_page(client, params)

        if status in (301, 302) and not items:
            # Redundant, because follow_redirects=True, but leave a hint if something odd happens
            logger.warning("Openverse returned %s redirect even after follow_redirects.", status)

        if not items:
            return []

        # Normalize and truncate to target_count
        normalized = []
        for it in items[: max(1, target_count)]:
            try:
                normalized.append(_map_item_to_normalized(it))
            except Exception as e:
                logger.warning("Openverse item mapping error: %s  item_snip=%r", e, dict(list(it.items())[:6]))
        return normalized
