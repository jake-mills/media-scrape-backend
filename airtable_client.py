# airtable_client.py
from __future__ import annotations

import os
import json
import urllib.parse
from typing import Dict, Any, List, Optional
import requests

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Videos & Images").strip()

if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
    missing = [k for k, v in {
        "AIRTABLE_API_KEY": AIRTABLE_API_KEY,
        "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
        "AIRTABLE_TABLE_NAME": AIRTABLE_TABLE_NAME,
    }.items() if not v]
    raise RuntimeError(f"Missing Airtable env var(s): {', '.join(missing)}")

ENCODED_TABLE = urllib.parse.quote(AIRTABLE_TABLE_NAME)
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ENCODED_TABLE}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

def _escape_for_formula(value: str) -> str:
    # O'Brien -> O''Brien inside single-quoted Airtable formula strings
    return value.replace("'", "''")

def find_by_source_url(source_url: str) -> Optional[Dict[str, Any]]:
    if not source_url:
        return None
    # Field name must match exactly: "Source URL"
    formula = "{Source URL} = '" + _escape_for_formula(source_url) + "'"
    resp = requests.get(
        AIRTABLE_URL,
        headers=HEADERS,
        params={"filterByFormula": formula, "maxRecords": 1},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    records: List[Dict[str, Any]] = data.get("records", [])
    return records[0] if records else None

def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"records": [{"fields": fields}], "typecast": True}
    resp = requests.post(
        AIRTABLE_URL,
        headers=HEADERS,
        data=json.dumps(payload),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    recs = data.get("records", [])
    if not recs:
        raise RuntimeError("Airtable insert returned no records.")
    return recs[0]
