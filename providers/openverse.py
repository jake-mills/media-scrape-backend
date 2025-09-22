# providers/openverse.py

from __future__ import annotations

import os
import random
from typing import Dict, List, Any, Tuple

import httpx


OPENVERSE_BASE = "https://api.openverse.engineering/v1/images"


class OpenverseError(RuntimeError):
    pass


def _auth_headers() -> Dict[str, str]:
    """
    Openverse requires:
      Authorization: Bearer <API_KEY>

    We read OPENVERSE_API_KEY from the environment. If it’s missing,
    raise a clear error so the API responds with a precise message.
    """
    api_key = os.getenv("OPENVERSE_API_KEY", "").strip()
    if not api_key:
        raise OpenverseError(
            "Missing OPENVERSE_API_KEY; set it in Render (Env Vars) and redeploy."
        )
    return {"Authorization": f"Bearer {api_key}"}


def _license_params(commercial_only: bool) -> Dict[str, str]:
    """
    When commercial_only=True, Openverse supports 'license_type=commercial'.
    Otherwise we let Openverse return any license.
    """
    return {"license_type": "commercial"} if commercial_only else {}


def _pick_page(seed: str | None, max_pages: int = 10) -> int:
    """
    Deterministic 'shuffle': if a runId is provided we use it as a seed so
    repeated runs choose the same page; otherwise pick a random page in 1..max_pages.
    """
    if seed:
        rnd = random.Random(seed)
        return rnd.randint(1, max_pages)
    return random.randint(1, max_pages)


def _normalize(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize one Openverse image result into our generic provider shape.
    Keys here are what airtable_client.py expects so it can map to your columns:
      - title
      - provider
      - source_url
      - thumbnail_url
      - creator
      - license
      - license_url
      - width
      - height
      - tags          (comma-separated string)
      - published     (ISO date or None)
    """
    tags = item.get("tags") or []
    tag_names = ", ".join(t.get("name") for t in tags if isinstance(t, dict) and t.get("name"))

    return {
        "title": item.get("title") or None,
        "provider": "Openverse",
        "source_url": item.get("foreign_landing_url") or item.get("url") or None,
        "thumbnail_url": item.get("thumbnail") or None,
        "creator": item.get("creator") or None,
        "license": item.get("license") or None,                  # e.g., "by", "cc0"
        "license_url": item.get("license_url") or None,
        "width": item.get("width") or None,
        "height": item.get("height") or None,
        "tags": tag_names or None,
        "published": item.get("created_on") or None,             # some records include this
    }


async def search_images(
    topic: str,
    target_count: int,
    *,
    commercial_only: bool = True,
    run_id: str | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Query Openverse images for `topic` and return up to `target_count` normalized items.
    We also return a list of 'notes' (strings) that the caller can record to help debugging.

    Raises OpenverseError for user-actionable issues (missing API key, 4xx from Openverse).
    """
    if target_count <= 0:
        return [], ["target_count was <= 0; nothing to fetch"]

    headers = _auth_headers()
    params: Dict[str, Any] = {
        "q": topic,
        "page_size": min(max(target_count, 1), 50),  # Openverse allows up to 50
        "page": _pick_page(run_id or topic, max_pages=10),
    }
    params.update(_license_params(commercial_only))

    notes: List[str] = []
    notes.append(f"Openverse params: {params}")

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(OPENVERSE_BASE, headers=headers, params=params)
        except httpx.HTTPError as e:
            raise OpenverseError(f"Network error contacting Openverse: {e}") from e

    if resp.status_code == 401:
        # Most common when the key is missing or wrong
        raise OpenverseError(
            "Openverse returned 401 Unauthorized. "
            "Verify OPENVERSE_API_KEY and that we send 'Authorization: Bearer <KEY>'."
        )

    if resp.status_code == 429:
        raise OpenverseError("Openverse rate limit (429). Try again later.")

    if resp.status_code >= 400:
        # Surface the exact URL to help diagnose parameter issues
        raise OpenverseError(
            f"Openverse error {resp.status_code}: {resp.text.strip()[:300]} "
            f"(url: {str(resp.request.url)})"
        )

    data = resp.json()
    results = data.get("results") or []
    items = [_normalize(r) for r in results]
    # Truncate to requested count
    items = items[:target_count]

    notes.append(f"Openverse returned {len(results)} results; using {len(items)}")

    return items, notes


# Public facade used by app.py
async def search(
    topic: str,
    target_count: int,
    media_mode: str,
    run_id: str | None,
    *,
    commercial_only: bool = True,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    app.py calls this entrypoint for any provider module.
    Openverse supports Images only; if 'Videos' is requested we return empty set with a note.
    """
    if media_mode.lower() != "images":
        return [], [f"Openverse supports Images only; got media_mode='{media_mode}'"]

    return await search_images(
        topic=topic,
        target_count=target_count,
        commercial_only=commercial_only,
        run_id=run_id,
    )
