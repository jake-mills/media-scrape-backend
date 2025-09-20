# providers/openverse.py

from typing import List, Dict, Any, Optional
import httpx

OPENVERSE_ENDPOINT = "https://api.openverse.engineering/v1/images/"

# We only request the fields we actually use.
DEFAULT_FIELDS = [
    "id",
    "title",
    "creator",
    "license",
    "license_version",
    "url",
    "thumbnail",
    "provider",
    "source",
    "foreign_landing_url",
]

HEADERS = {
    "Accept": "application/json",
    # A polite UA helps with some CDNs and rate-limiters.
    "User-Agent": "media-scrape-backend/1.0 (+https://media-scrape-backend.onrender.com)",
}


def _to_airtable_item(
    obj: Dict[str, Any], *, topic: str, run_id: str, search_dates: str
) -> Dict[str, Any]:
    """Convert one Openverse item into your Airtable row 'fields' dict."""
    src_url = obj.get("foreign_landing_url") or obj.get("url")
    thumb_url = obj.get("thumbnail") or obj.get("url")

    lic = (obj.get("license") or "").upper()
    lic_ver = obj.get("license_version")
    copyright_val = f"CC {lic} {lic_ver}".strip() if lic else "Unknown"

    provider_name = obj.get("provider") or "Openverse"
    if isinstance(provider_name, str):
        provider_name = provider_name.title()

    return {
        "Run ID": run_id,
        "Media Type": "Image",
        "Provider": provider_name,
        "Title": obj.get("title") or "(untitled)",
        "Source URL": src_url,
        "Thumbnail": [{"url": thumb_url}] if thumb_url else [],
        "Search Topics Used": topic,
        "Search Dates Used": search_dates or "",
        "Published/Created": None,  # Openverse public API doesn't include creation date
        "Copyright": copyright_val,
    }


async def fetch_openverse_async(
    session: httpx.AsyncClient,
    *,
    topic: str,
    search_dates: Optional[str],
    target_count: int,
    run_id: str,
    timeout_seconds: float = 10.0,
) -> List[Dict[str, Any]]:
    """
    Fetch images from Openverse and return a list of Airtable-ready 'fields' dicts.

    Note: the Openverse public API does not currently expose a creation-date filter.
    We record the user's requested date range in 'Search Dates Used' but cannot filter
    the results server-side by date.
    """
    # httpx >=0.28 requires a fully-specified Timeout or a single default.
    timeout = httpx.Timeout(
        timeout_seconds,
        connect=timeout_seconds,
        read=timeout_seconds,
        write=timeout_seconds,
        pool=timeout_seconds,
    )

    # Over-fetch a little to improve chances of non-duplicate rows, but stay ≤50 (API limit).
    page_size = min(max(target_count * 3, 5), 50)

    params = {
        "q": topic,
        "page_size": page_size,
        "fields": ",".join(DEFAULT_FIELDS),
    }

    try:
        resp = await session.get(
            OPENVERSE_ENDPOINT,
            params=params,
            headers=HEADERS,
            timeout=timeout,
            follow_redirects=True,
        )
    except Exception:
        # The caller logs provider errors; we just return empty on hard failures.
        return []

    if resp.status_code != 200:
        # Non-200 → let the caller log the provider warning and continue.
        return []

    data = resp.json()
    results = data.get("results") or []

    out: List[Dict[str, Any]] = []
    for obj in results:
        out.append(
            _to_airtable_item(
                obj, topic=topic, run_id=run_id, search_dates=search_dates or ""
            )
        )
        if len(out) >= target_count:
            break

    return out
