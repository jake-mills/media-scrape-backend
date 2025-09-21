# airtable_client.py
"""
Minimal Airtable helper (no third-party client).
Env vars used:
  AIRTABLE_API_KEY
  AIRTABLE_BASE_ID
  AIRTABLE_TABLE_NAME
"""

from __future__ import annotations
import os
import json
from typing import Dict, Any, Optional, List
import requests

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", "").strip()

_API_ROOT = "https://api.airtable.com/v0"

def _check_env() -> None:
    missing = [k for k, v in {
        "AIRTABLE_API_KEY": AIRTABLE_API_KEY,
        "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
        "AIRTABLE_TABLE_NAME": AIRTABLE_TABLE_NAME,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env var(s): {', '.join(missing)}")

def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

def _url() -> str:
    return f"{_API_ROOT}/{AIRTABLE_BASE_ID}/{requests.utils.requote_uri(AIRTABLE_TABLE_NAME)}"

def _escape_for_formula(value: str) -> str:
    # Airtable formulas escape single quotes by doubling them
    return value.replace("'", "''")

def find_by_source_url(source_url: str) -> Optional[Dict[str, Any]]:
    """
    Look up a record by exact match on the 'Source_URL' field.
    Returns the first matching record (dict) or None.
    """
    _check_env()
    formula = f"{{Source_URL}} = '{_escape_for_formula(source_url)}'"
    params = {"filterByFormula": formula, "maxRecords": 1}
    resp = requests.get(_url(), headers=_headers(), params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    records: List[Dict[str, Any]] = data.get("records", [])
    return records[0] if records else None

def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a single row (record) into Airtable. 'fields' should be a dict
    mapping column name -> value (strings, numbers, lists, etc).
    Returns the created record (dict).
    """
    _check_env()
    payload = {"records": [{"fields": fields}], "typecast": True}
    resp = requests.post(_url(), headers=_headers(), data=json.dumps(payload), timeout=20)
    resp.raise_for_status()
    data = resp.json()
    created = data.get("records", [])
    if not created:
        raise RuntimeError("Insert succeeded but response had no records")
    return created[0]
