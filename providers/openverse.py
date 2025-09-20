# providers/openverse.py
from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ---------- helpers ----------

YEAR_RANGE_RX = re.compile(r"^\s*(\d{4})\s*[-–]\s*(\d{4})\s*$")

def _parse_year_range(search_dates: Optional[str]) -> Optional[Tuple[int, int]]:
    """Accepts formats like '2000-2010' or '1995–2005'. Returns (start, end) or None."""
    if not search_dates:
        return None
    m = YEAR_RANGE_RX.match(str(search_dates))
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    if a > b:
        a, b = b, a
    return a, b

def _first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v:
            s = str(v).strip()
            if s:
                return s
    return None

def _infer_year(item: Dict[str, Any]) -> Optional[int]:
    """
    Try very gently to find a year in typical Openverse fields.
    We DO NOT fail if we can't find one.
    """
    # Some providers expose 'created_on', 'created', or 'source', or a date-like 'title'.
    for key in ("created_on", "created", "date", "publish_date"):
        v = item.get(key)
        if isinstance(v, str) and re.search(r"\d{4}", v):
            try:
                # Try first 4-digit match
                yr = int(re.search(r"(\d{4})", v).group(1))  # type: ignore[arg-type]
                if 1800 <= yr <= datetime.utcnow().year + 1:
                    return yr
            except Exception:
                pass

    # Sometimes a year hides inside 'title' or 'description'
    for key in ("title", "description"):
        v = item.get(key)
        if isinstance(v, str):
            m = re.search(r"(\d{4})", v)
            if m:
                yr = int(m.group(1))
                if 1800 <= yr <= datetime.utcnow().year + 1:
                    return yr
    return None

def _in_year_range(item: Dict[str, Any], yr_range: Optional[Tuple[int, int]]) -> bool:
    if yr_range is None:
        return True
    yr = _infer_year(item)
    if yr is None:
        # If user asked for a date range and we cannot infer a year, skip it quietly.
        return False
    return yr_range[0] <= yr <= yr_range[1]

def _normalize_row(
    item: Dict[str, Any],
    topic: str,
    search_dates: Optional[str],
    run_id: str,
) -> Dict[str, Any]:
    """
    Return a row compatible with the Airtable schema used elsewhere.
    """
    # Openverse fields vary; common ones:
    #  - 'title', 'foreign_landing_url', 'url', 'thumbnail', 'provider'
    #  - license fields: 'license', 'license_version', 'license_url'
    title = _first_nonempty(item.get("title"), "Untitled")
    landing = _first_nonempty(item.get("foreign_landing_url"), item.get("url"))
    thumb = _first_nonempty(item.get("thumbnail"))
    provider = _first_nonempty(item.get("provider"), "Openverse")

    # Build a friendly copyright string when possible
    lic = (item.get("license") or "") .upper()
    lic_ver = item.get("license_version") or ""
    lic_url = item.get("license_url") or ""
    copyright_text = "Unknown"
    if lic:
        copyright_text = f"{lic} {lic_ver}".strip()
        if lic_url:
            copyright_text += f" ({lic_url})"

    # Best-effort date
    pub_year = _infer_year(item)
    published = str(pub_year) if pub_year else ""

    return {
        "Index": None,  # Autonumber in Airtable
        "Media Type": "Image",
        "Provider": provider,
        "Thumbnail": [{"url": thumb}] if thumb else [],
        "Title": title,
        "Source URL": landing or "",
        "Search Topics Used": topic,
        "Search Dates Used": search_dates or "",
        "Published/Created": published,
        "Copyright": copyright_text,
        "Run ID": run_id,
        "Notes": "",
    }

# ---------- main fetcher ----------

async def fetch_openverse_async(
    *,
    topic: str,
    search_dates: Optional[str],
    target_count: int,
    run_id: str,
    use_precision: bool = False,   # accepted but currently unused; kept for compatibility
    logger: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """
    Query Openverse Images API and return normalized rows.

    Must accept the exact keyword parameters the rest of your app sends:
      topic, search_dates, target_count, run_id, (optional) use_precision, logger
    """
    log = (logger.info if logger else lambda *a, **k: None)
    warn = (logger.warning if logger else lambda *a, **k: None)

    # Defensive bounds
    page_size = max(1, min(int(target_count or 1), 20))

    yr_range = _parse_year_range(search_dates)

    # API docs suggest: https://api.openverse.engineering/v1/images/?q=...&page_size=...
    base_url = "https://api.openverse.engineering/v1/images/"
    params = {
        "q": topic,
        "page_size": page_size,
        # We avoid brittle filters; titles, descriptions, tags are searched by default.
    }

    headers = {
        # Some CDNs show 403/blocked to generic bots; send a real UA.
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0)

    try:
        async with httpx.AsyncClient(
            headers=headers,
            follow_redirects=True,
            timeout=timeout,
        ) as client:
            resp = await client.get(base_url, params=params)
            if resp.status_code != 200:
                warn(
                    "Openverse unexpected status %s. Returning empty result.",
                    resp.status_code,
                )
                return []

            data = resp.json()
            results: List[Dict[str, Any]] = list(data.get("results") or [])

            # Post-filter by year range if requested
            filtered = [it for it in results if _in_year_range(it, yr_range)]

            # Normalize & dedupe by Source URL
            rows: List[Dict[str, Any]] = []
            seen = set()
            for it in filtered:
                row = _normalize_row(it, topic=topic, search_dates=search_dates, run_id=run_id)
                key = row.get("Source URL") or row.get("Title")
                if key and key not in seen:
                    seen.add(key)
                    rows.append(row)

            # We only need up to target_count
            final_rows = rows[:target_count]
            log("Openverse returned %d items (after filtering)", len(final_rows))
            return final_rows

    except httpx.TimeoutException:
        warn("Openverse timeout; returning empty result.")
        return []
    except Exception as e:
        warn("Provider openverse failed: %s", e)
        return []
