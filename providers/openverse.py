# providers/openverse.py
import os
import logging
import aiohttp
from aiohttp import ClientTimeout
from typing import Any, Dict, List, Optional

log = logging.getLogger("providers.openverse")


OPENVERSE_IMAGES_ENDPOINT = "https://api.openverse.engineering/v1/images/"


async def _ensure_session(session: aiohttp.ClientSession | None) -> tuple[aiohttp.ClientSession, bool]:
    if session is None or session.closed:
        return aiohttp.ClientSession(timeout=ClientTimeout(total=20)), True
    return session, False


def _pick_title(item: Dict[str, Any], fallback: str) -> str:
    # Openverse can provide title, alt_text, or nothing
    title = (item.get("title") or item.get("alt_text") or "").strip()
    return title if title else fallback


def _first_source_url(item: Dict[str, Any]) -> Optional[str]:
    # Prefer the direct URL if available, else fall back to foreign_landing_url
    url = item.get("url") or item.get("foreign_landing_url")
    return url


async def fetch_openverse_async(
    *,
    query: str,
    search_dates: Optional[str] = None,   # Not supported directly by the API; kept for consistency
    license_type: str = "commercial",     # "commercial" filters to commercially-usable content
    page_size: int = 10,
    session: aiohttp.ClientSession | None = None,
    run_id: Optional[str] = None,
    use_precision: bool = False,          # reserved toggle; Openverse doesn't expose a precision flag
) -> List[Dict[str, Any]]:
    """
    Query Openverse Images API and normalize to:
      { "title": str, "provider": "Openverse", "source_url": str }

    Returns a list with length <= page_size.
    """
    s, created = await _ensure_session(session)
    try:
        params: Dict[str, Any] = {
            "q": query,
            "license_type": license_type,  # keep default "commercial" so results are safer to reuse
            "page_size": max(1, min(int(page_size), 20)),  # Openverse allows up to ~500; keep small by default
        }

        if os.getenv("DEBUG_OPENVERSE"):
            log.info("[Openverse] run_id=%s q=%r params=%s", run_id, query, params)

        async with s.get(OPENVERSE_IMAGES_ENDPOINT, params=params, allow_redirects=True) as resp:
            if resp.status != 200:
                log.warning("[Openverse] unexpected status %s", resp.status)
                return []

            data = await resp.json()

        results = data.get("results") or []
        normalized: List[Dict[str, Any]] = []
        for item in results:
            source_url = _first_source_url(item)
            if not source_url:
                continue

            title = _pick_title(item, fallback=query)
            normalized.append({
                "title": title,
                "provider": "Openverse",
                "source_url": source_url,
            })

        if os.getenv("DEBUG_OPENVERSE"):
            log.info("[Openverse] Returning %d item(s)", len(normalized))

        return normalized[: max(1, int(page_size))]
    except Exception as e:
        log.warning("Provider openverse failed: %s", e)
        return []
    finally:
        if created:
            await s.close()
