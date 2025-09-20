# providers/openverse.py
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

from aiohttp import ClientResponse, ClientSession, ClientTimeout

OPENVERSE_BASE = "https://api.openverse.engineering/v1/images/"

_year_pair = re.compile(r"(?P<y1>\d{4})\D+(?P<y2>\d{4})")

def _parse_year_range(search_dates: Optional[str]) -> Optional[tuple[int, int]]:
    """
    Accepts "2000-2010", "2000–2010", "2000 — 2010", etc.
    Returns (2000, 2010) or None.
    """
    if not search_dates:
        return None
    m = _year_pair.search(search_dates)
    if not m:
        return None
    y1 = int(m.group("y1"))
    y2 = int(m.group("y2"))
    lo, hi = (y1, y2) if y1 <= y2 else (y2, y1)
    return (lo, hi)

def _year_or_none(created_on: Optional[str]) -> Optional[int]:
    """
    Openverse image objects may include 'created_on' (ISO date). Extract year if present.
    """
    if not created_on:
        return None
    m = re.match(r"(\d{4})", created_on.strip())
    return int(m.group(1)) if m else None

def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Openverse fields to our normalized schema expected upstream.
    Falls back carefully when fields are absent.
    """
    title = raw.get("title") or raw.get("foreign_landing_url") or "Untitled"
    # In Openverse, 'foreign_landing_url' is the page where the media lives.
    source_url = raw.get("foreign_landing_url") or raw.get("url") or ""
    # Prefer thumbnail if provided, fall back to 'url'
    thumb = raw.get("thumbnail") or raw.get("url") or ""
    license_id = raw.get("license")  # e.g., 'cc-by', 'cc0', etc.
    provider = raw.get("provider") or "Openverse"
    created_on = raw.get("created_on") or raw.get("created_at")  # some datasets use created_at

    return {
        "title": title,
        "source_url": source_url,
        "thumbnail_url": thumb,
        "copyright": license_id or "Unknown",
        "provider": provider,
        "published": created_on or "",
        "media_type": "Image",
    }

async def _fetch_page(
    session: ClientSession,
    logger,
    q: str,
    page: int,
    page_size: int = 50,
) -> Optional[Dict[str, Any]]:
    """
    Fetch a single page from Openverse images API.
    Returns parsed JSON dict or None on error.
    """
    params = {
        "q": q,
        "page": page,
        "page_size": page_size,
    }

    # 15s total timeout prevents Render log warning and long hangs
    timeout = ClientTimeout(total=15)

    try:
        async with session.get(OPENVERSE_BASE, params=params, timeout=timeout) as resp:
            if resp.status == 301 or resp.status == 302:
                # Shouldn't happen for the API hostname, but handle gracefully.
                logger.warning("Openverse unexpected redirect (%s). Returning empty page.", resp.status)
                return None
            if resp.status != 200:
                text = await resp.text()
                logger.warning("Openverse non-200 (%s): %s", resp.status, text[:400])
                return None
            return await resp.json()
    except Exception as e:
        logger.warning("Openverse page fetch failed: %s", e)
        return None

async def fetch_openverse_async(
    *,
    topic: str,
    search_dates: Optional[str],
    target_count: int,
    run_id: str,
    session: ClientSession,
    logger,
) -> List[Dict[str, Any]]:
    """
    Primary provider entry point (signature matches app.py call sites).

    Returns a list of normalized image dicts with at most `target_count` items.
    Safe on errors: logs and returns [].
    """
    # Defensive guards
    topic = (topic or "").strip()
    if not topic:
        logger.warning("Openverse: empty topic, run_id=%s", run_id)
        return []

    yr_range = _parse_year_range(search_dates)
    need = max(0, int(target_count) if isinstance(target_count, int) else 0)
    if need == 0:
        return []

    results: List[Dict[str, Any]] = []

    # We’ll pull a couple pages max to keep latency low on free Render
    # (50 per page; adjust if you routinely need more than 100)
    max_pages = 3
    page = 1

    while page <= max_pages and len(results) < need:
        data = await _fetch_page(session, logger, q=topic, page=page, page_size=50)
        page += 1
        if not data:
            continue

        items = data.get("results") or []
        if not isinstance(items, list) or not items:
            continue

        for raw in items:
            # Year filter if the API provides a year-like field
            if yr_range:
                y = _year_or_none(raw.get("created_on") or raw.get("created_at"))
                if y is not None:
                    lo, hi = yr_range
                    if y < lo or y > hi:
                        continue  # outside requested window

            norm = _normalize_item(raw)
            # Discard obviously incomplete rows
            if not norm["source_url"]:
                continue

            results.append(norm)
            if len(results) >= need:
                break

    # Log what we’re returning (count only, to keep logs tidy)
    logger.info("Openverse run_id=%s returning %d items", run_id, len(results))
    return results
