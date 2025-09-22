# providers/openverse.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
import httpx

# Normalized item keys expected by airtable_client.py
# required: source_url, title, provider, media_type
# optional: thumbnail_url, published_date, license, notes

OPENVERSE_IMAGES_API = "https://api.openverse.engineering/v1/images/"

_LICENSE_MAP: Dict[str, Tuple[str, str]] = {
    "cc0":           ("CC0",                          "https://creativecommons.org/publicdomain/zero/1.0/"),
    "cc-by":         ("Creative Commons BY",          "https://creativecommons.org/licenses/by/4.0/"),
    "cc-by-sa":      ("Creative Commons BY-SA",       "https://creativecommons.org/licenses/by-sa/4.0/"),
    "cc-by-nd":      ("Creative Commons BY-ND",       "https://creativecommons.org/licenses/by-nd/4.0/"),
    "cc-by-nc":      ("Creative Commons BY-NC",       "https://creativecommons.org/licenses/by-nc/4.0/"),
    "cc-by-nc-sa":   ("Creative Commons BY-NC-SA",    "https://creativecommons.org/licenses/by-nc-sa/4.0/"),
    "cc-by-nc-nd":   ("Creative Commons BY-NC-ND",    "https://creativecommons.org/licenses/by-nc-nd/4.0/"),
    "pdm":           ("Public Domain Mark",           "https://creativecommons.org/share-your-work/public-domain/pdm/"),
}

def _format_license(code: Optional[str], version: Optional[str]) -> Tuple[str, str]:
    if not code:
        return ("", "")
    code = code.lower().strip()
    human, url = _LICENSE_MAP.get(code, (code.upper(), ""))
    if version and human and version not in human:
        human = f"{human} {version}"
    return (human, url)

def _first_nonempty(*vals: Optional[str]) -> str:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _copyright(rec: Dict[str, Any]) -> str:
    creator = _first_nonempty(rec.get("creator"), rec.get("attribution"))
    code = _first_nonempty(rec.get("license"))
    version = _first_nonempty(rec.get("license_version"))
    license_url = _first_nonempty(rec.get("license_url"))

    human, canonical = _format_license(code, version)
    parts: List[str] = []
    if creator:
        parts.append(f"By {creator}")
    if human:
        parts.append(human)
    url = license_url or canonical
    if url:
        parts.append(url)
    return " — ".join(parts)

def _normalize(rec: Dict[str, Any], topic: str) -> Optional[Dict[str, Any]]:
    src = _first_nonempty(rec.get("foreign_landing_url"), rec.get("url"))
    if not src:
        return None
    title = _first_nonempty(rec.get("title"), rec.get("alt_text"), "(untitled)")
    thumb = _first_nonempty(rec.get("thumbnail"), rec.get("thumbnail_url"), rec.get("url"))
    published = _first_nonempty(rec.get("source_created_at"), rec.get("created_on"))
    return {
        "source_url": src,
        "title": title,
        "provider": "Openverse",
        "media_type": "Images",
        "thumbnail_url": thumb,
        "published_date": published,
        "license": _copyright(rec),
        "notes": f"Query: {topic}",
    }

async def search_openverse_images(
    *,
    topic: str,
    target_count: int = 20,
    license_type: Optional[str] = "commercial",
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "q": topic,
        "page_size": max(1, min(int(target_count * 3), 200)),
    }
    if license_type:
        params["license_type"] = license_type

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(OPENVERSE_IMAGES_API, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results") or []
    out: List[Dict[str, Any]] = []
    for rec in results:
        norm = _normalize(rec, topic)
        if norm:
            out.append(norm)
        if len(out) >= target_count:
            break
    return out
