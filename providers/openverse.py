# providers/openverse.py

from typing import List, Dict, Any, Optional
import httpx
import logging

logger = logging.getLogger("openverse_provider")

OPENVERSE_ENDPOINT = "https://api.openverse.org/v1/images"

def _normalize_item(
    obj: Dict[str, Any],
    *,
    topic: str,
    run_id: str,
    search_dates: Optional[str]
) -> Optional[Dict[str, Any]]:
    """
    Normalize an Openverse result into the Airtable row fields dict.
    Returns None if essential fields missing.
    """

    # source URL must exist
    source_url = obj.get("foreign_landing_url") or obj.get("url")
    if not source_url:
        return None

    thumb_url = obj.get("thumbnail") or obj.get("url")

    title = obj.get("title") or "(untitled)"
    provider = obj.get("provider") or "Openverse"
    # Clean provider name
    if isinstance(provider, str):
        provider = provider.title()

    license_ = obj.get("license", "")
    license_ver = obj.get("license_version", "")
    if license_:
        if license_ver:
            copyright_str = f"{license_.upper()} {license_ver}"
        else:
            copyright_str = license_.upper()
    else:
        copyright_str = ""

    return {
        "Run ID": run_id,
        "Media Type": "Image",
        "Provider": provider,
        "Title": title,
        "Source URL": source_url,
        "Thumbnail": [{"url": thumb_url}] if thumb_url else [],
        "Search Topics Used": topic,
        "Search Dates Used": search_dates or "",
        "Published/Created": None,  # Openverse doesn't guarantee this
        "Copyright": copyright_str,
    }


async def fetch_openverse_async(
    *,
    topic: str,
    search_dates: Optional[str],
    target_count: int,
    run_id: str,
    use_precision: Optional[bool] = False,
) -> List[Dict[str, Any]]:
    """
    Fetch up to target_count images from Openverse for the given topic & optional search_dates.
    Returns list of normalized items.
    """

    if target_count <= 0:
        return []

    # Overfetch multiplier to allow for dropping items
    overfetch = 3
    page_size = min(max(target_count * overfetch, 5), 50)

    headers = {
        "Accept": "application/json",
        "User-Agent": "media-scrape-backend/1.0 (+https://media-scrape-backend.onrender.com)",
    }

    # Timeout: must explicitly set all four parts
    timeout = httpx.Timeout(
        connect=10.0,
        read=20.0,
        write=20.0,
        pool=5.0
    )

    params = {
        "q": topic,
        "page_size": page_size,
    }

    # Add optional detail or other filter if API supports, but here simplest
    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
            resp = await client.get(OPENVERSE_ENDPOINT, params=params)
    except httpx.TimeoutException as e:
        logger.warning(f"Openverse fetch timeout: {e}")
        return []
    except Exception as e:
        logger.warning(f"Openverse fetch unexpected error: {e}")
        return []

    if resp.status_code == 301 or resp.status_code == 302:
        # Probably missing slash or redirect; since follow_redirects=True should solve, but log anyway
        logger.warning(f"Openverse returned redirect status {resp.status_code} for URL {resp.url}")
    if resp.status_code != 200:
        # log body snippet for debugging
        body_snip = ""
        try:
            body_snip = resp.text[:200]
        except Exception:
            pass
        logger.warning(f"Openverse non-200 status {resp.status_code}: {body_snip}")
        return []

    data = None
    try:
        data = resp.json()
    except Exception as e:
        logger.warning(f"Openverse JSON parse error: {e}")
        return []

    results = data.get("results")
    if results is None:
        logger.warning("Openverse JSON missing 'results' field")
        return []

    normalized: List[Dict[str, Any]] = []
    for obj in results:
        item = _normalize_item(obj, topic=topic, run_id=run_id, search_dates=search_dates)
        if item is not None:
            normalized.append(item)
        if len(normalized) >= target_count:
            break

    return normalized
