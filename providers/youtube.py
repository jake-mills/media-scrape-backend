import os, httpx
from typing import List, Dict, Any, Optional

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

async def fetch_youtube_async(topic: str, max_results: int = 25,
                              published_after: Optional[str] = None,
                              published_before: Optional[str] = None) -> List[Dict[str, Any]]:
    if not YOUTUBE_API_KEY:
        return []
    params = {
        "part": "snippet",
        "q": topic,
        "type": "video",
        "maxResults": min(max_results, 50),
        "key": YOUTUBE_API_KEY,
        "safeSearch": "none"
    }
    if published_after:
        params["publishedAfter"] = published_after
    if published_before:
        params["publishedBefore"] = published_before
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get("https://www.googleapis.com/youtube/v3/search", params=params)
        r.raise_for_status()
        data = r.json()

    out: List[Dict[str, Any]] = []
    for item in data.get("items", []):
        try:
            vid = item["id"]["videoId"]
            snip = item.get("snippet", {})
            title = snip.get("title") or ""
            thumbs = snip.get("thumbnails", {})
            picked = thumbs.get("high") or thumbs.get("medium") or thumbs.get("default") or {}
            thumb = picked.get("url", "")
            published_at = snip.get("publishedAt", "")
            out.append({
                "title": title,
                "url": f"https://www.youtube.com/watch?v={vid}",
                "thumbnail": thumb,
                "type": "Video",
                "provider": "YouTube",
                "published_at": published_at,
                "copyright": "YouTube Standard"
            })
        except Exception:
            continue
    return out
