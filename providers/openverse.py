# providers/openverse.py
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("provider.openverse")

OPENVERSE_BASE = "https://api.openverse.org/v1/images/"

# Helper: parse "2000–2010" or "2000-2010" into ("2000","2010")
_YEAR_RANGE_RE = re.compile(r"^\s*(\d{4})\s*[-–]\s*(\d{4})\s*$")

def _parse_year_range(search_dates: Optional[str]) -> Optional[str]:
    """
    Convert a human string like '2000–2010' to Openverse 'created:2000..2010' filter.
    Returns None if not a range.
    """
    if not search_dates:
        return None
    m = _YEAR_RANGE_RE.match(search_dates)
    if not m:
        return None
    start, end = m.group(1), m.group(2)
    return f"created:{start}..{end}"

def _norm_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a single Openverse image record to your Airtable row shape.
    Only fields you actually use downstream are populated.
    """
    title = raw.get("title") or raw.get("id") or "Untitled"
    source_url = raw.get("url") or raw.get("foreign_landing_url") or ""
    thumbnail = raw.get("thumbnail") or raw.get("thumbnail_url") or raw.get("url") or ""

    # Openverse dates are usually 'date_uploaded' or 'created_at'
    published = raw.get("date_uploaded") or raw.get("created_at") or ""

    license_ = raw.get("license") or ""
    license_version = raw.get("license_version") or ""
    copyright_str = (f"CC {license_.upper()} {license_version}".strip() if license_ else "").strip()

    provider = "Openverse"  # stable label for your Provider column

    return {
        # Airtable target columns:
        "Media Type": "Image",
        "Provider": provider,
        "Title": title,
        "Source URL": source_url,
        "Thumbnail": {
            "id": raw.get("id") or "",
            "url": thumbnail,
            "filename": "",  # you can populate later if you actually download
        },
        "Published/Created": published,
        "Copyright": copyright_str,
    }

async def _fetch_page(
    client: httpx.AsyncClient,
    *,
    q: str,
    filter_expr: Optional[str],
    page: int,
    page_size: int,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "q": q,
        "page": page,
        "page_size": page_size,
        # keep results useful/commons-usable by default; tweak if you want
        "license_type": "commercial",   # optional; narrow to commercial-friendly
    }
    if filter_expr:
        params["filter"] = filter_expr

    resp = await client.get(OPENVERSE_BASE, params=params)
    if resp.status_code != 200:
        log.warning("Openverse unexpected status %s. Returning empty result.", resp.status_code)
        return []
    data = resp.json()
    results = data.get("results") or []
    return [ _norm_item(r) for r in results ]

async def fetch_openverse_async(
    *,
    topic: str,
    search_dates: Optional[str] = None,
    target_count: int = 1,
    run_id: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
    **_ignore: Any,  # swallow any future/unknown kwargs (e.g., use_precision)
) -> List[Dict[str, Any]]:
    """
    Fetch images from Openverse for a query topic and optional year range.
    Always keyword-only; safe defaults; never raises—returns [] on any upstream failure.

    Parameters expected from app.py (names must match app’s call):
      - topic: str
      - search_dates: Optional[str]  # e.g., "2000–2010" or "2000-2010"
      - target_count: int            # how many items you want (best-effort)
      - run_id: Optional[str]        # forwarded for logging (optional)
      - client: Optional[httpx.AsyncClient] # reuse if the app already has one
      - **_ignore: Any               # tolerate extra kwargs without breaking
    """
    if not topic:
        return []

    filter_expr = _parse_year_range(search_dates)
    desired = max(1, int(target_count or 1))
    page_size = min(50, max(1, desired))  # Openverse supports up to 500; 50 is a safe default
    pages = (desired + page_size - 1) // page_size

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=10.0, read=10.0, write=10.0),
            follow_redirects=True,
            headers={
                "User-Agent": "MediaScrape/1.0 (+https://example.invalid)",  # be polite
                "Accept": "application/json",
            },
        )
        close_client = True

    rows: List[Dict[str, Any]] = []
    try:
        tasks = [
            _fetch_page(
                client,
                q=topic,
                filter_expr=filter_expr,
                page=i + 1,
                page_size=page_size,
            )
            for i in range(pages)
        ]
        for chunk in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(chunk, Exception):
                log.warning("Openverse page fetch error: %s", chunk)
                continue
            rows.extend(chunk)
            if len(rows) >= desired:
                break

        # trim to exactly target_count
        if len(rows) > desired:
            rows = rows[:desired]

        return rows
    except Exception as e:
        log.warning("Provider openverse failed: %s", e)
        return []
    finally:
        if close_client:
            await client.aclose()
