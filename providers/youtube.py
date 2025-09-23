# providers/youtube.py
from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import os
import httpx

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"

class YouTubeClient:
    """
    Minimal YouTube Data API v3 search client (videos).
    Requires env YOUTUBE_API_KEY.
    """

    def __init__(self, api_key: Optional[str] = None, timeout: float = 20.0):
        self.api_key = api_key or YOUTUBE_API_KEY
        if not self.api_key:
            raise RuntimeError("Missing YOUTUBE_API_KEY for YouTube provider.")
        self.timeout = timeout

    async def search_videos(self, topic: str, target_count: int = 10) -> Tuple[str, List[Dict]]:
        max_results = max(1, min(50, target_count))
        params = {
            "key": self.api_key,
            "part": "snippet",
            "q": topic,
            "type": "video",
            "maxResults": max_results,
            "order": "relevance",
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(YOUTUBE_SEARCH_URL, params=params)
            if r.status_code == 403:
                raise RuntimeError("YouTube API quota or key error (HTTP 403). Check YOUTUBE_API_KEY & quotas.")
            r.raise_for_status()
            data = r.json()

        items: List[Dict] = []
        for row in data.get("items", []):
            vid = (row.get("id") or {}).get("videoId")
            snippet = row.get("snippet") or {}
            if not vid:
                continue

            source_url = f"https://www.youtube.com/watch?v={vid}"
            title = snippet.get("title") or ""
            published = snippet.get("publishedAt") or ""
            thumbs = (snippet.get("thumbnails") or {})
            thumb = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")

            items.append({
                "media_type": "Videos",
                "provider": "YouTube",
                "title": title,
                "source_url": source_url,
                "thumbnail": thumb,
                "published": published,
                "copyright": "",   # YouTube doesn't return license text in search
                "notes": "",
            })

        return ("YouTube", items)
