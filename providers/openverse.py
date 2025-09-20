import os, httpx
from typing import List, Dict, Any

OPENVERSE_ENDPOINT = os.getenv("OPENVERSE_ENDPOINT", "https://api.openverse.engineering/v1/images")

async def fetch_openverse_async(topic: str, page_size: int = 30) -> List[Dict[str, Any]]:
    params = {"q": topic, "page_size": min(page_size, 50)}
    headers = {"Accept": "application/json"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(OPENVERSE_ENDPOINT, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
    out: List[Dict[str, Any]] = []
    for item in data.get("results", []):
        try:
            title = item.get("title") or ""
            url = item.get("url") or ""
            thumb = item.get("thumbnail") or item.get("preview") or ""
            lic = item.get("license") or ""
            published = item.get("created_at") or ""
            out.append({
                "title": title,
                "url": url,
                "thumbnail": thumb,
                "type": "Image",
                "provider": "Openverse",
                "published_at": published[:10] if published else "",
                "copyright": lic.upper() if lic else "CC"
            })
        except Exception:
            continue
    return out
