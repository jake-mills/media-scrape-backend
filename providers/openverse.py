# providers/openverse.py
import os
import logging
from typing import List, Dict, Any, Tuple
import httpx

logger = logging.getLogger("uvicorn.error")

# Enable extra logging if DEBUG_OPENVERSE=1 in environment
DEBUG_OPENVERSE = os.getenv("DEBUG_OPENVERSE", "0") not in ("", "0", "false", "False")

OPENVERSE_BASE = "https://api.openverse.engineering/v1/images/"

def _to_airtable_row(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a single Openverse API result into Airtable row format.
    """
    return {
        "Title": item.get("title") or "",
        "Provider": "Openverse",
        "Source_URL": item.get("url") or "",
        "Thumbnail": [{"url": item.get("thumbnail")}] if item.get("thumbnail") else [],
        "Media Type": "Image",
        "Copyright": (item.get("license") or "").upper(),
        "Published": (item.get("created_on") or "")[:10],  # first 10 chars = YYYY-MM-DD
    }

def _build_params(query: str, start_year: int, end_year: int, limit: int) -> Dict[str, Any]:
    """
    Construct API parameters for Openverse search.
    Openverse doesn’t support explicit year filters, so we bias with year text in query.
    """
    year_hint = f"{start_year}..{end_year}" if start_year and end_year else ""
    q = f"{query} {year_hint}".strip()
    return {
        "q": q,
        "license_type": "all",
        "page_size": max(1, min(int(limit or 10), 50)),  # Openverse caps at 50
    }

async def fetch_openverse_async(
    *,
    query: str,
    start_year: int = None,
    end_year: int = None,
    limit: int = 10,
    run_id: str = "manual-test",
    **_  # absorb extra keyword args (prevents crashes on unexpected kwargs)
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Query Openverse API asynchronously and return results in Airtable-ready format.
    Returns:
      rows: List of dicts for Airtable insertion
      meta: Diagnostic info (ok/count/params/errors/etc.)
    """
    params = _build_params(query, start_year, end_year, limit)
    headers = {"Accept": "application/json"}
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        try:
            resp = await client.get(OPENVERSE_BASE, params=params, headers=headers)
        except Exception as e:
            logger.warning("[OV %s] request failed: %r", run_id, e)
            return [], {"ok": False, "error": str(e), "params": params}

    status = resp.status_code
    ctype = resp.headers.get("content-type", "")

    if DEBUG_OPENVERSE:
        logger.info(
            "[OV %s] GET %s status=%s ctype=%s params=%s",
            run_id, OPENVERSE_BASE, status, ctype.split(";")[0], params
        )

    if status != 200:
        snippet = (resp.text or "")[:200].replace("\n", " ")
        logger.warning("[OV %s] non-200 (%s). Body[200]=%s", run_id, status, snippet)
        return [], {"ok": False, "status": status, "body_snippet": snippet, "params": params}

    try:
        data = resp.json()
    except Exception as e:
        snippet = (resp.text or "")[:200].replace("\n", " ")
        logger.warning("[OV %s] json decode failed: %r body[200]=%s", run_id, e, snippet)
        return [], {"ok": False, "status": status, "body_snippet": snippet, "params": params}

    results = data.get("results") or []
    if DEBUG_OPENVERSE:
        preview = {}
        if results:
            first = results[0]
            preview = {
                "title": first.get("title"),
                "url": first.get("url"),
                "thumbnail": first.get("thumbnail"),
                "license": first.get("license"),
            }
        logger.info("[OV %s] count=%d preview=%s", run_id, len(results), preview)

    rows = [_to_airtable_row(it) for it in results[: params["page_size"]]]
    return rows, {"ok": True, "count": len(rows), "params": params}
