# airtable_client.py
# Lightweight Airtable REST client (no pyairtable dependency).
# Works on Python 3.13 and with FastAPI/pydantic v2.

from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional

import httpx

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "").strip()

if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
    raise RuntimeError(
        "Airtable env not set. Please define AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME."
    )

_API_ROOT = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{httpx.utils.quote(AIRTABLE_TABLE_NAME)}"
_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}


async def find_by_source_url(source_url: str) -> Optional[Dict[str, Any]]:
    """
    Returns the first row whose {Source_URL} exactly matches `source_url`,
    or None if not found.
    """
    # Use a parameterized formula so we don’t fight URL encoding issues.
    params = {
        "filterByFormula": f"{{Source_URL}} = '{source_url.replace(\"'\", \"\\'\")}'",
        "maxRecords": 1,
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(_API_ROOT, headers=_HEADERS, params=params)
        r.raise_for_status()
        data = r.json()
        records = data.get("records", [])
        return records[0] if records else None


async def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inserts a single row. `fields` are the column names/values from your schema.
    Returns the created Airtable record.
    """
    payload = {"records": [{"fields": fields}], "typecast": True}
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(_API_ROOT, headers=_HEADERS, content=json.dumps(payload))
        r.raise_for_status()
        data = r.json()
        recs = data.get("records", [])
        if not recs:
            raise RuntimeError("Airtable insert returned no records.")
        return recs[0]
