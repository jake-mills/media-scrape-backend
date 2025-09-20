# providers/openverse.py
from typing import List, Dict, Any
import httpx
import urllib.parse
import logging

OPENVERSE_BASE = "https://api.openverse.engineering/v1/images/"

# Unified record the rest of your pipeline expects
def _map_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # Be defensive about field names — Openverse responses commonly include these keys.
    # If a key is missing, we fall back gracefully.
    title = item.get("title") or item.get("id") or "Untitled"
    thumb = item.get("thumbnail") or item.get("thumbnail_url") or ""
    # "url" is the direct media URL in Openverse; many UIs prefer "foreign_landing_url"
    src = item.get("url") or item.get("foreign_landing_url") or ""
    lic = item.get("license")
    lic_ver = item.get("license_version")
    copyright_str = (
        f"{lic}-{lic_ver}".upper() if lic and lic_ver else (lic.upper() if lic else "")
    )

    return {
        "Title": title,
        "Thumbnail": {"url": thumb} if thumb else None,
        "Source URL": src,
        "Media Type": "Image",
        "Provider": "Openverse",
        "Copyright": copyright_str,
        # "Published/Created" is not reliably available; leave blank unless item has it
        "Published/Created": item.get("created_on") or item.get("created_at") or None,
        # Keep the raw payload in case we need to debug or enrich later
        "_raw": item,
    }

async def fetch_openverse_async(
    topic: str,
    date_range: str,        # not directly supported by the API; we include it in q
    target_count: int,
    logger: logging.Logger | None = None,
) -> List[Dict[str, Any]]:
    """
    Search Openverse for images.
    Returns a list of unified records ready for Airtable insertion.
    """

    # Build query. Openverse docs show querying with "q" against /v1/images/.
    # We include the date text in the query string to help relevance when present.
    q_parts = [topic.strip()]
    if date_range:
        q_parts.append(date_range.strip())
    q = " ".join(p for p in q_parts if p)

    params = {
        "q": q,
        # Ask for a little extra and slice; avoids a second call if some items are filtered out.
        "page_size": max(1, min(50, target_count * 3)),
    }

    # Proper timeout per httpx docs: either a single float or all four parts.
    # We'll set all four explicitly to avoid the error you hit.
    timeout = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=5.0)

    headers = {
        "Accept": "application/json",
        # Friendly User-Agent; not documented as required, but helpful in logs.
        "User-Agent": "MediaScrape/1.0 (+https://render.com)",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # IMPORTANT: trailing slash, as documented for /v1/images/
            resp = await client.get(OPENVERSE_BASE, params=params, headers=headers)
            if resp.status_code == 301:
                if logger:
                    logger.warning("Openverse returned 301; check URL includes trailing slash.")
                # Try following once with the documented path (already correct)
                resp = await client.get(OPENVERSE_BASE, params=params, headers=headers)

            if resp.status_code == 429:
                if logger:
                    logger.warning("Openverse rate-limited (429). Returning empty result.")
                return []

            if resp.status_code // 100 != 2:
                if logger:
                    logger.warning(
                        f"Provider openverse failed: HTTP {resp.status_code} body={resp.text[:300]}"
                    )
                return []

            data = resp.json()

            # Docs show usage as images.data.results; some deployments return top-level "results".
            results = None
            if isinstance(data, dict):
                results = data.get("results")
                if results is None and "data" in data and isinstance(data["data"], dict):
                    results = data["data"].get("results")

            if not results:
                return []

            mapped = [_map_item(it) for it in results]
            # Keep only items with a usable Source URL; then trim to target_count.
            filtered = [m for m in mapped if m["Source URL"]]
            return filtered[: max(1, target_count)]

    except httpx.TimeoutException:
        if logger:
            logger.warning("Openverse request timed out.")
        return []
    except Exception as exc:
        if logger:
            logger.warning(f"Openverse unexpected error: {exc}")
        return []
