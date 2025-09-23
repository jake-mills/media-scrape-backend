# providers/openverse.py
from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import os
import httpx

OPENVERSE_BASE = "https://api.openverse.org/v1"

class OpenverseClient:
    """
    Async Openverse client (images).
    - Works without auth.
    - If OPENVERSE_TOKEN is set, we send Authorization: Bearer <token>.
    """

    def __init__(self, token: Optional[str] = None, timeout: float = 20.0):
        self.token = token
        self.timeout = timeout

    async def search_images(self, topic: str, target_count: int = 10) -> Tuple[str, List[Dict]]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        page_size = min(max(target_count, 1), 50)
        url = f"{OPENVERSE_BASE}/images"
        params = {"q": topic, "page_size": page_size}

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code == 401:
                raise RuntimeError("Openverse 401 Unauthorized — set OPENVERSE_TOKEN in your env if required.")
            r.raise_for_status()
            data = r.json()

        items: List[Dict] = []
        for row in data.get("results", []):
            title = row.get("title") or ""
            landing = row.get("foreign_landing_url") or row.get("url") or ""
            thumb = row.get("thumbnail") or row.get("url") or ""
            creator = row.get("creator") or ""
            license_code = row.get("license") or ""

            if creator and license_code:
                copyright_str = f"{creator} — {license_code.upper()}"
            elif license_code:
                copyright_str = license_code.upper()
            else:
                copyright_str = creator

            items.append({
                "media_type": "Images",
                "provider": "Openverse",
                "title": title,
                "source_url": landing,
                "thumbnail": thumb,
                "published": "",   # Often not available for images
                "copyright": copyright_str,
                "notes": "",
            })

        return ("Openverse", items)
