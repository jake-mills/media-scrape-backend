import os
from typing import List, Dict, Any
import httpx
import logging

OPENVERSE_ENDPOINT = "https://api.openverse.engineering/v1"
OPENVERSE_KEY = os.getenv("OPENVERSE_API_KEY", "").strip()

async def search(topic: str, media_mode: str = "images", target_count: int = 5) -> List[Dict[str, Any]]:
    media_mode = media_mode.lower()
    if media_mode.startswith("image"):
        mode = "images"
    elif media_mode.startswith("video"):
        mode = "images"
    else:
        mode = "images"

    url = f"{OPENVERSE_ENDPOINT}/{mode}/"
    params = {"q": topic, "page_size": max(1, min(50, int(target_count)))}
    headers = {}
    if OPENVERSE_KEY:
        headers["Authorization"] = f"Bearer {OPENVERSE_KEY}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params, headers=headers)
        if resp.status_code != 200:
            logging.error("Openverse error %s: %s", resp.status_code, resp.text)
            return []
        data = resp.json()

    out: List[Dict[str, Any]] = []
    for r in data.get("results", []):
        out.append({
            "id": r.get("id"),
            "title": r.get("title") or "",
            "url": r.get("url") or "",
            "thumbnail": r.get("thumbnail") or "",
            "published": r.get("created_at") or "",
            "copyright": (r.get("license") or ""),
        })
    return out
