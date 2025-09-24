import os
import httpx
import logging
from typing import List, Dict, Any

OPENVERSE_ENDPOINT = "https://api.openverse.engineering/v1"
OPENVERSE_KEY = os.getenv("OPENVERSE_API_KEY")

async def search(topic: str, media_mode: str = "images", target_count: int = 5) -> List[Dict[str, Any]]:
    """
    Async search against Openverse. media_mode: "images" or "audio".
    Returns list of Openverse result dicts (we normalize in app.py).
    """
    media_mode = media_mode.lower()
    if media_mode not in ("images", "audio"):
        raise ValueError(f"Unsupported media_mode: {media_mode}")

    url = f"{OPENVERSE_ENDPOINT}/{media_mode}/"
    params = {"q": topic, "page_size": target_count}
    headers = {}
    if OPENVERSE_KEY:
        headers["Authorization"] = f"Bearer {OPENVERSE_KEY}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params, headers=headers)
        if resp.status_code != 200:
            logging.error("Openverse error %s: %s", resp.status_code, resp.text)
            return []
        data = resp.json()

    return data.get("results", []) or []
