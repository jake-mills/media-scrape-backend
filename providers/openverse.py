# providers/openverse.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import httpx


OPENVERSE_API = os.getenv("OPENVERSE_API", "https://api.openverse.org/v1/images")
REQUEST_TIMEOUT = float(os.getenv("OPENVERSE_TIMEOUT", "15"))  # seconds
# Slightly over-request to improve chances of meeting target_count after filtering
OVERSHOOT_FACTOR = float(os.getenv("OPENVERSE_OVERSHOOT", "2.0"))
MAX_PAGE_SIZE = 100


def _parse_year_range(search_dates: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    Accepts formats like:
      "2000-2010", "1999–2005", "2008", "", None
    Returns (start_year, end_year) where any may be None.
    """
    if not search_dates:
        return (None, None)
    cleaned = (
        search_dates.replace("–", "-")
        .replace("—", "-")
        .replace(" ", "")
        .strip()
    )
    if not cleaned:
        return (None, None)

    if "-" in cleaned:
        a, b = cleaned.split("-", 1)
        try:
            start = int(a)
        except ValueError:
            start = None
        try:
            end = int(b)
        except ValueError:
            end = None
        return (start, end)

    # single year
    try:
        y = int(cleaned)
        return (y, y)
    except ValueError:
        return (None, None)


def _year_from_record(rec: Dict[str, Any]) -> Optional[int]:
    """
    Best-effort: Openverse image records don’t always include a capture/publication year.
    Common fields: 'source', 'title', 'creator', 'thumbnail', 'url', 'license', 'license_version',
    'provider', 'foreign_landing_url', 'id', etc. Some have 'taken_at' or 'created_on'.
    """
    for key in ("taken_at", "created_on", "created_at", "taken_on", "capture_date"):
        val = rec.get(key)
        if isinstance(val, str) and len(val) >= 4:
            try:
                return int(val[:4])
            except ValueError:
                pass
    return None


def _license_string(rec: Dict[str, Any]) -> str:
    lic = (rec.get("license") or "").upper()
    ver = (rec.get("license_version") or "").strip()
    if lic and ver:
        return f"{lic} {ver}"
    return lic or ""


def _normalize_item(rec: Dict[str, Any], topic: str, search_dates: Optional[str]) -> Dict[str, Any]:
    """
    Convert an Openverse record to your pipeline's normalized dict
    (fields your Airtable mapping expects).
    NOTE: app.py adds `Index` and `Run ID`.
    """
    title = rec.get("title") or "Untitled"
    thumb = rec.get("thumbnail") or rec.get("url")
    source_url = rec.get("foreign_landing_url") or rec.get("url") or ""
    created_display = rec.get("created_on") or rec.get("taken_at") or ""

    return {
        "Media Type": "Image",
        "Provider": "Openverse",
        "Title": title,
        "Source URL": source_url,
        "Thumbnail": {
            "id": rec.get("id"),
            "url": thumb,
            "filename": "",  # You can populate later when you download files, if needed
        },
        "Copyright": _license_string(rec),
        "Published/Created": created_display,
        "Search Topics Used": topic,
        "Search Dates Used": search_dates or "",
        # "Run ID" and "Index" are added upstream in app.py
    }


async def fetch_openverse_async(
    topic: str,
    target_count: int,
    search_dates: Optional[str] = None,
    use_precision: Optional[bool] = None,  # not used by Openverse; kept for API symmetry
) -> List[Dict[str, Any]]:
    """
    Query Openverse Images API and return up to `target_count` normalized items.
    This function **never raises** for network or API errors — it logs and returns [] instead.
    """
    # Avoid pathological values
    if target_count <= 0:
        return []

    desired = max(1, target_count)
    page_size = min(int(desired * OVERSHOOT_FACTOR), MAX_PAGE_SIZE)

    # Build query params
    params = {
        "q": topic,
        "page_size": page_size,
        # You can add additional filters here if you want, e.g. 'mature': 'false'
    }

    headers = {
        # A desktop UA helps reduce some 403/blocked responses
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
    }

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, headers=headers) as client:
            resp = await client.get(OPENVERSE_API, params=params)
            # Gracefully handle rate-limit / forbidden
            if resp.status_code in (401, 402, 403, 429):
                print(
                    f"[WARN] Openverse blocked or rate-limited "
                    f"(status {resp.status_code}). Returning empty result."
                )
                return []
            if resp.status_code >= 500:
                print(f"[WARN] Openverse server error {resp.status_code}. Returning empty result.")
                return []
            if resp.status_code != 200:
                print(f"[WARN] Openverse unexpected status {resp.status_code}. Returning empty result.")
                return []

            data = resp.json()
    except Exception as e:
        print(f"[WARN] Openverse request failed: {e}")
        return []

    results = data.get("results") or []
    if not isinstance(results, list):
        print("[WARN] Openverse payload missing 'results' list.")
        return []

    start_year, end_year = _parse_year_range(search_dates)

    filtered: List[Dict[str, Any]] = []
    for rec in results:
        # Optional local year filter
        if start_year or end_year:
            y = _year_from_record(rec)
            if y is None:
                # If no year present, keep it — or drop it. Here we keep to avoid over-filtering.
                pass
            else:
                if start_year and y < start_year:
                    continue
                if end_year and y > end_year:
                    continue

        filtered.append(_normalize_item(rec, topic=topic, search_dates=search_dates))

        if len(filtered) >= desired:
            break

    return filtered[:desired]
