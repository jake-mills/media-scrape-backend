# providers/openverse.py

from typing import List, Dict, Any, Optional
import httpx

OPENVERSE_ENDPOINT = "https://api.openverse.engineering/v1/images/"

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
    "User-Agent": "media-scrape-backend/1.0 (+https://media-scrape-backend.onrender.com)",
}


def _to_airtable_item(
    obj: Dict[str, Any], *, topic: str, run_id: str, search_dates: str
) -> Dict[str, Any]:
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
        "Published/Created": None,
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
    timeout = httpx.Timeout(
        timeout_seconds,
        connect=timeout_seconds,
        read=timeout_seconds,
        write=timeout_seconds,
        pool=timeout_seconds,
    )

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
        return []

    if resp.status_code != 200:
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
