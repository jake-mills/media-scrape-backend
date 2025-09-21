# providers/openverse.py
import os
import logging
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("providers.openverse")

OPENVERSE_BASE_URL = os.getenv("OPENVERSE_BASE_URL", "https://api.openverse.org/v1")
USER_AGENT = os.getenv("OPENVERSE_USER_AGENT", "media-scrape-backend/1.0 (+https://render.com)")


class OpenverseProvider:
    """
    Minimal Openverse image search wrapper returning a normalized list of dictionaries
    that the app's insert pipeline expects.

    Returned dict keys:
      - title
      - provider
      - source_url
      - thumbnail_url
      - creator
      - license
      - license_url
      - width
      - height
      - tags (list[str])
    """

    def __init__(self, debug: bool = False, timeout_s: float = 15.0) -> None:
        self.debug = debug
        self.timeout_s = timeout_s
        self.base_url = OPENVERSE_BASE_URL.rstrip("/")

    async def search(
        self,
        query: str,
        *,
        license_type: Optional[str] = "commercial",
        page_size: int = 20,
        search_dates: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of Openverse image results.

        Args:
          query: search text
          license_type: e.g. "commercial" (Openverse supported values include
                        "commercial", "noncommercial", "modification", etc.)
          page_size: 1..500 (Openverse caps at 500, we keep modest defaults)
          search_dates: free-form hint like "2000-2010" (appends to query to bias results)

        Returns:
          List of normalized item dicts (see class docstring).
        """
        if search_dates:
            # Openverse doesn't expose a strict date filter; nudging the query helps.
            q = f"{query} {search_dates}"
        else:
            q = query

        params: Dict[str, Any] = {
            "q": q,
            "page_size": max(1, min(int(page_size), 100)),  # keep sane upper bound
            "license_type": license_type or "",
        }

        url = f"{self.base_url}/images/"

        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }

        if self.debug:
            log.info("Openverse request: %s params=%s", url, params)

        async with httpx.AsyncClient(timeout=self.timeout_s, headers=headers) as client:
            resp = await client.get(url, params=params)
            if self.debug:
                log.info("Openverse status=%s", resp.status_code)

            resp.raise_for_status()
            data = resp.json()

        results = data.get("results", []) if isinstance(data, dict) else []
        normalized: List[Dict[str, Any]] = []

        for item in results:
            # Defensive pulls with fallbacks; Openverse fields vary slightly by record.
            title = _first_nonempty(
                item.get("title"),
                item.get("alt_text"),
                item.get("source"),   # sometimes present
            )

            source_url = _first_nonempty(
                item.get("url"),
                item.get("foreign_landing_url"),
            )

            # If we can't link back to a source, skip it (we dedupe by this key).
            if not source_url:
                continue

            thumb = _first_nonempty(
                item.get("thumbnail"),
                item.get("thumbnail_url"),
                item.get("detail_url"),  # not ideal, but a fallback image
            )

            creator = _first_nonempty(
                item.get("creator"),
                item.get("creator_url"),
                item.get("source"),  # sometimes the site/source is all we have
            )

            license_code = item.get("license") or None
            license_url = item.get("license_url") or None

            width = _safe_int(item.get("width"))
            height = _safe_int(item.get("height"))

            tags_raw = item.get("tags") or []
            if isinstance(tags_raw, list):
                # Openverse tags are typically list[{"name": "cat"}, ...]
                tags = [
                    t.get("name")
                    for t in tags_raw
                    if isinstance(t, dict) and t.get("name")
                ]
            else:
                tags = []

            normalized.append(
                {
                    "title": title,
                    "provider": "Openverse",
                    "source_url": source_url,
                    "thumbnail_url": thumb,
                    "creator": creator,
                    "license": license_code,
                    "license_url": license_url,
                    "width": width,
                    "height": height,
                    "tags": tags,
                }
            )

        if self.debug and normalized:
            log.info("Openverse first normalized item: %s", _truncate(normalized[0], 600))
            log.info("Openverse returned %d normalized items", len(normalized))

        return normalized


# ---------------------------- helpers ---------------------------- #

def _first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _truncate(obj: Any, max_len: int) -> str:
    s = str(obj)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."
