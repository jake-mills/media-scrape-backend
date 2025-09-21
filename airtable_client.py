import os
from typing import Optional, Dict, Any, List
import requests

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", "Media")
AIRTABLE_API_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"

_session = requests.Session()
_session.headers.update(
    {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }
)

def _escape_for_airtable(value: str) -> str:
    """
    Inside Airtable formulas, single quotes inside a quoted string are represented by two single quotes.
    Example: O'Brien  ->  O''Brien
    """
    return value.replace("'", "''")

def find_by_source_url(source_url: str) -> Optional[Dict[str, Any]]:
    """
    Look up a row by exact Source_URL match.
    """
    if not source_url:
        return None

    escaped = _escape_for_airtable(source_url)
    # Build filter formula without f-strings to avoid parser/escape pitfalls
    formula = "{Source_URL} = '" + escaped + "'"

    resp = _session.get(
        AIRTABLE_API_URL,
        params={"filterByFormula": formula, "maxRecords": 1},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    records: List[Dict[str, Any]] = data.get("records", [])
    return records[0] if records else None

def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new row with the given fields.
    """
    payload = {"records": [{"fields": fields}], "typecast": True}
    resp = _session.post(AIRTABLE_API_URL, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()
