# providers/openverse.py
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


OPENVERSE_SEARCH_URL = "https://api.openverse.engineering/v1/images/"

# Build a friendly UA. Some CDNs rate-limit empty/unknown UAs.
UA = "media-scrape-backend/1.0 (+https://media-scrape-backend.onrender.com)"


def _license_label(data: Dict[str, Any]) -> str:
    """
    Compose a human-readable copyright label from Openverse fields.
    """
    lic = (data.get("license") or "").upper()
    ver = (data.get("license_version") or "").strip()
    if lic and ver:
        return f"CC {lic}-{ver}"
    if lic:
        return f"CC {lic}"
    return "Unknown"


def _first_non_empty(*vals: Optional[str]) -> str:
    for v in vals:
        if v:
            return v
    return ""


def _thumbnail_fields(data: Dict[str, Any]) -> Dict[str, str]:
    thumb_url = data.get("thumbnail") or ""
    # Try to derive a filename from the direct URL (if any)
    filename = ""
    try:
        if thumb_url:
            filename = thumb_url.rstrip("/").split("/")[-1].split("?")[0]
    except Exception:
        filename = ""
    # An id is useful for dedupe/debug; use Openverse id
    return {
        "id": str(data.get("id") or ""),
        "url": thumb_url,
        "filename": filename,
    }


async def _fetch_page(
    session: aiohttp.ClientSession,
    query: str,
    *,
    page: int = 1,
    page_size: int = 20,
    license_filter: Optional[str] = None,
    timeout: aiohttp.ClientTimeout,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Fetch one page from Openverse. Returns (results, next_page).
    next_page is None when there are no further pages.
    """
    params = {
        "q": query,
        "page": page,
        "page_size": page_size,
        # Restrict to images only (this endpoint is already images).
        # Add any additional filters you need here:
        # "extension": "jpg,png",
    }
    if license_filter:
        params["license"] = license_filter

    async with session.get(
        OPENVERSE_SEARCH_URL,
        params=params,
        timeout=timeout,
        allow_redirects=True,
    ) as resp:
        # Openverse returns 200 with JSON body on success.
        if resp.status != 200:
            # Let caller decide; they can treat as transient and stop.
            return [], None
        data = await resp.json(loads=None, content_type=None)
        results = data.get("results") or []
        # Pagination hint
        next_page = page + 1 if data.get("result_count") else None
        # If results empty, stop
        if not results:
            next_page = None
        return results, next_page


async def fetch_openverse_async(
    *,
    topic: str,
    target_count: int,
    run_id: str,
    # Optional filters:
    search_dates: Optional[str] = None,   # kept for signature parity; not used by Openverse directly
    license_filter: Optional[str] = None,
    page_size: int = 50,
    request_timeout_sec: float = 12.0,
) -> List[Dict[str, Any]]:
    """
    Query Openverse Images API for `topic` until we collect up to `target_count` items.
    Returns a list of dicts already mapped to the Airtable field names your pipeline expects.

    Notes:
    - Openverse's public API does not support a direct "year range" filter for images,
      so `search_dates` is not applied server-side. If you want a soft bias, you could
      append the years to the query string, e.g., "wildlife 2000..2010". For now we keep
      topic unchanged to avoid degrading results.
    """
    results: List[Dict[str, Any]] = []
    if target_count <= 0:
        return results

    timeout = aiohttp.ClientTimeout(
        total=request_timeout_sec, connect=5, sock_connect=5, sock_read=request_timeout_sec
    )

    headers = {"User-Agent": UA, "Accept": "application/json"}

    async with aiohttp.ClientSession(headers=headers) as session:
        page = 1
        while len(results) < target_count:
            raw_items, next_page = await _fetch_page(
                session,
                query=topic,
                page=page,
                page_size=min(page_size, max(1, target_count * 2)),  # grab a few extra to filter
                license_filter=license_filter,
                timeout=timeout,
            )

            if not raw_items:
                break

            # Map Openverse items → Airtable-friendly rows
            for item in raw_items:
                title = _first_non_empty(
                    item.get("title"),
                    item.get("creator"),
                    # last resort: derive from URL
                    (item.get("url") or "").rstrip("/").split("/")[-1].split("?")[0],
                )
                source_url = _first_non_empty(item.get("foreign_landing_url"), item.get("url"))

                mapped = {
                    # **These keys match what your response preview & Airtable insert expect**
                    "Title": title or "",
                    "Provider": "Openverse",
                    "Source URL": source_url or "",
                    "Media Type": "Image",
                    "Thumbnail": _thumbnail_fields(item),
                    "Copyright": _license_label(item),
                }

                # Minimal dedupe on URL within this batch
                already = any(r.get("Source URL") == mapped["Source URL"] for r in results)
                if not already:
                    results.append(mapped)
                    if len(results) >= target_count:
                        break

            if not next_page:
                break
            page = next_page

    return results
