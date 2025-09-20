from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncio
from aiohttp import ClientSession, ClientError

OPENVERSE_API = "https://api.openverse.org/v1/images"


def _coalesce(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v:
            return v
    return None


def _license_string(item: Dict[str, Any]) -> str:
    # Examples: "CC-BY 4.0", "CC0", "PDM"
    lic = (item.get("license") or "").upper()
    ver = item.get("license_version") or ""
    return (f"{lic} {ver}").strip()


def _normalize_record(
    item: Dict[str, Any],
    topic: str,
    search_dates: str,
) -> Dict[str, Any]:
    """
    Return a dict shaped to be easy for your Airtable mapping layer.
    (Your app's insert step can pick/rename only the fields it needs.)
    """
    return {
        # Core columns used later in your pipeline
        "Title": _coalesce(item.get("title"), "Untitled"),
        "Source URL": _coalesce(item.get("foreign_landing_url"), item.get("url")),
        "Thumbnail": {
            "id": item.get("id"),
            "url": item.get("thumbnail"),
            "filename": None,
        },
        "Copyright": _license_string(item),
        "Media Type": "Image",
        "Provider": _coalesce(item.get("provider"), item.get("source"), "Openverse"),
        # Sometimes Openverse doesn’t provide a reliable date; keep None if missing
        "Published/Created": _coalesce(
            item.get("created_on"),
            item.get("upload_date"),
            item.get("last_synced_with_source"),
        ),
        # Helpful passthroughs (your app can drop these if it already sets them)
        "Search Topics Used": topic,
        "Search Dates Used": search_dates,
        # Raw (safe subset) in case you want to inspect later
        "_ov_id": item.get("id"),
        "_ov_url": item.get("url"),
        "_ov_landing": item.get("foreign_landing_url"),
        "_ov_provider": item.get("provider"),
        "_ov_source": item.get("source"),
    }


async def fetch_openverse_async(
    *,
    topic: str,
    target_count: int,
    search_dates: str,
    use_precision: bool,  # currently unused, kept for interface parity
    session: ClientSession,
) -> List[Dict[str, Any]]:
    """
    Query Openverse for images matching `topic` and return up to `target_count`
    normalized records.

    This function is defensive: network failures or unexpected payloads
    return an empty list rather than bubbling a 500 up to FastAPI.
    """
    # Page size max is 50; we only fetch what we need once.
    page_size = max(1, min(50, int(target_count or 1)))

    params: Dict[str, Any] = {
        "q": topic,
        "page_size": page_size,
        # Default to safe content; adjust if you add a setting later.
        "mature": "false",
        # Keep all license types; your Airtable has a Copyright column.
        # You can filter to CC-only with: "license_type": "cc"
    }

    headers: Dict[str, str] = {
        # If you later add an Openverse API key, include:
        # "Authorization": f"Bearer {os.environ['OPENVERSE_API_KEY']}"
    }

    try:
        async with session.get(OPENVERSE_API, params=params, headers=headers, timeout=30) as resp:
            if resp.status != 200:
                # Log-lite shape for upstream logger (FastAPI will capture prints)
                print(f"[openverse] HTTP {resp.status} for topic={topic!r} params={params}")
                return []
            data = await resp.json()

    except (ClientError, asyncio.TimeoutError) as e:
        print(f"[openverse] request failed: {e}")
        return []
    except Exception as e:
        # JSON decode or other unexpected issues
        print(f"[openverse] unexpected error: {e}")
        return []

    results = data.get("results") or []
    out: List[Dict[str, Any]] = []

    for item in results:
        try:
            out.append(_normalize_record(item, topic, search_dates))
            if len(out) >= page_size:
                break
        except Exception as e:
            # Skip single bad items; continue
            print(f"[openverse] normalize error for id={item.get('id')}: {e}")
            continue

    return out
