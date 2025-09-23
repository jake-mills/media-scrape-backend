# airtable_client.py
from __future__ import annotations
import os
from typing import Any, Dict, Optional
import httpx

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]  # e.g., "Videos & Images"

BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

def _table_url() -> str:
    return f"{BASE_URL}/{AIRTABLE_TABLE_NAME}"

def _escape_formula_value(val: str) -> str:
    return val.replace("'", "\\'") if isinstance(val, str) else val

def find_by_source_url(url: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return the first record if {Source URL} matches exactly, else None."""
    if not url:
        return None
    safe = _escape_formula_value(url)
    params = {"filterByFormula": f"{{Source URL}}='{safe}'", "maxRecords": 1}
    with httpx.Client(timeout=20) as client:
        r = client.get(_table_url(), params=params, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
    records = data.get("records", [])
    return records[0] if records else None

def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Create a single Airtable record. Keys must match column names verbatim."""
    payload = {"fields": fields}
    with httpx.Client(timeout=20) as client:
        r = client.post(_table_url(), json=payload, headers=HEADERS)
        r.raise_for_status()
        return r.json()
