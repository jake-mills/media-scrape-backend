# airtable_client.py
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from pyairtable import Table
from pyairtable.formulas import match  # safe formula builder

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", "")

if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
    raise RuntimeError(
        "Missing Airtable configuration. Ensure AIRTABLE_API_KEY, "
        "AIRTABLE_BASE_ID, and AIRTABLE_TABLE_NAME are set."
    )

# Single Table instance reused across calls
table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

log = logging.getLogger("airtable_client")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _norm_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize incoming fields so they match your Airtable column names exactly.
    Adjust this mapping if your base uses different column headers.
    """
    # Map common payload keys -> Airtable columns (adjust as needed)
    mapping = {
        "title": "Title",
        "provider": "Provider",
        "source_url": "Source_URL",
        "thumbnail_url": "Thumbnail_URL",
        "creator": "Creator",
        "license": "License",
        "license_url": "License_URL",
        "width": "Width",
        "height": "Height",
        "tags": "Tags",
        "topic": "Topic",
        "media_mode": "Media_Mode",
        "run_id": "Run_ID",
    }

    out: Dict[str, Any] = {}
    for k, v in fields.items():
        col = mapping.get(k, k)  # default to the original key if no mapping
        out[col] = v
    return out


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def find_by_source_url(source_url: str) -> Optional[Dict[str, Any]]:
    """
    Return the existing Airtable row (record dict) for a given Source_URL, or None.
    Uses pyairtable.formulas.match to avoid manual string escaping.
    """
    try:
        formula = match({"Source_URL": source_url})
        rec = table.first(formula=formula)
        if rec:
            log.info("Found existing Airtable record id=%s for Source_URL=%s", rec["id"], source_url)
        return rec
    except Exception as e:
        log.warning("Airtable lookup failed for %s: %s", source_url, e)
        return None


def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a new row. Callers should pass semantic keys; we’ll map to Airtable columns.
    Example input:
        {
          "title": "Wildlife",
          "provider": "Openverse",
          "source_url": "https://example.com/image.jpg",
          "media_mode": "Images",
          "run_id": "swagger-test-openverse-OK",
          ...
        }
    """
    cleaned = _norm_fields(fields)
    try:
        created = table.create(cleaned)
        log.info("Created Airtable record id=%s for Source_URL=%s",
                 created.get("id"), cleaned.get("Source_URL"))
        return created
    except Exception as e:
        # Bubble up with context — your FastAPI handler can return a 500 with this message
        raise RuntimeError(f"Airtable insert failed: {e}") from e


def upsert_by_source_url(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience: if a record exists for Source_URL, return it; otherwise insert.
    """
    src = fields.get("source_url") or fields.get("Source_URL")
    if not src:
        raise ValueError("upsert_by_source_url requires 'source_url' (or 'Source_URL') in fields")

    existing = find_by_source_url(src)
    if existing:
        return existing
    return insert_row(fields)
