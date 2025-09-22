# providers/openverse.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
import httpx

# NormalizedItem keys used by airtable_client.py:
#   required: source_url, title, provider, media_type
#   optional: thumbnail_url, published_date, license, notes

OPENVERSE_IMAGES_API = "https://api.openverse.engineering/v1/images/"

# Map Openverse license codes to human names and canonical URLs
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
    """
    Turn an Openverse license code + version into (human_label, canonical_url).
    Falls back gracefully if unknown.
    """
    if not code:
        return ("", "")
    code = code.lower()
    human, url = _LICENSE_MAP.get(code, (code.upper(), ""))

    # Append version if present (e.g., "Creative Commons BY 4.0")
    if version and human:
        if version not in human:
            human = f"{human} {version}"
    return (human, url)

def _compose_copyright(rec: Dict[str, Any]) -> str:
    """
    Build a clean, human-readable copyright/attribution string.
      Example: "By Jane Doe — Creative Commons BY 4.0 — https://creativecommons.org/licenses/by/4.0/"
    """
    creator = (rec.get("creator") or rec.get("attribution") or "").strip()
    code = (rec.get("license") or "").strip().lower() or None
    version = (rec.get("license_version") or "").strip() or None
    url_from_api = (rec.get("license_url") or "").strip()

    human, canonical = _format_license(code, version)
    parts: List[str] = []
    if creator:
        parts.append(f"By {creator}")
    if human:
        parts.append(human)
    # prefer API-provided license_url; fallback to canonical mapping
    url = url_from_api or canonical
    if url:
        parts.append(url)
    return " — ".join(parts)

def _first_nonempty(*vals: Optional[str]) -> str:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _normalize_image(rec: Dict[str, Any], *, topic: str) -> Optional[Dict[str, Any]]:
    """
    Convert one Openverse image record into a NormalizedItem for Airtable mapping.
    """
    # Landing page is best "source" (publisher page); fallback to direct URL.
    source_url = _first_nonempty(rec.get("foreign_landing_url"), rec.get("url"))
    if not source_url:
        return None

    title = _first_nonempty(rec.get("title"), rec.get("alt_text"), "(untitled)")
    thumb = _first_nonempty(rec.get("thumbnail"), rec.get("thumbnail_url"), rec.get("url"))
    # Try both API-provided timestamps; many images have only one (or none).
    published = _first_nonempty(rec.get("source_created_at"), rec.get("created_on"))

    return {
        "source_url": source_url,
        "title": title,
        "provider": "Openverse",
        "media_type": "Images",          # Your Airtable uses this exact label
        "thumbnail_url": thumb,
        "published_date": published,     # Lands in "Published/Created"
        "license": _compose_copyright(rec),  # Lands in "Copyright"
        "notes": f"Query: {topic}",
    }

async def search_openverse_images(
    *,
    topic: str,
    target_count: int = 20,
    license_type: Optional[str] = "commercial",  # you can set None to allow any
) -> List[Dict[str, Any]]:
    """
    Fetch up to target_count normalized image records from Openverse.
    """
    params: Dict[str, Any] = {
        "q": topic,
        "page_size": max(1, min(int(target_count * 3), 200)),  # overfetch a bit for de-dup downstream
    }
    if license_type:
        params["license_type"] = license_type  # e.g., "commercial"

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(OPENVERSE_IMAGES_API, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results") or []
    out: List[Dict[str, Any]] = []
    for rec in results:
        norm = _normalize_image(rec, topic=topic)
        if norm:
            out.append(norm)
        if len(out) >= target_count:
            break
    return out
