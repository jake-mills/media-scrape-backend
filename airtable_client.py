# airtable_client.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx


# ---- config / env helpers ---------------------------------------------------

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


AIRTABLE_API_KEY: str = _require_env("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID: str = _require_env("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME: str = _require_env("AIRTABLE_TABLE_NAME")  # e.g. 'Videos & Images'

# URL-encode the table name so spaces/& etc. are safe in the path
_TABLE_NAME_ENC = quote(AIRTABLE_TABLE_NAME, safe="")
_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"

_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

_HTTP_TIMEOUT = 20.0
_MAX_RETRIES = 3


def _table_url() -> str:
    return f"{_BASE_URL}/{_TABLE_NAME_ENC}"


def _escape_formula_value(val: str) -> str:
    # Airtable formula literal in single quotes: escape single quotes with backslash
    return val.replace("'", r"\'") if isinstance(val, str) else val


def _request_with_retries(method: str, url: str, **kwargs) -> httpx.Response:
    """
    Tiny retry loop for 429/5xx and transient network errors.
    """
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
                resp = client.request(method, url, headers=_HEADERS, **kwargs)
            # Raise for non-2xx so we can check status
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status in (429, 500, 502, 503, 504) and attempt < _MAX_RETRIES:
                time.sleep(0.8 * attempt)  # simple backoff
                continue
            raise
        except httpx.RequestError:
            if attempt < _MAX_RETRIES:
                time.sleep(0.8 * attempt)
                continue
            raise


# ---- public helpers ---------------------------------------------------------

def find_by_source_url(url: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Return the first record if {Source URL} matches exactly, else None.
    """
    if not url:
        return None

    safe = _escape_formula_value(url)
    params = {
        "filterByFormula": f"{{Source URL}}='{safe}'",
        "maxRecords": 1,
    }

    r = _request_with_retries("GET", _table_url(), params=params)
    data = r.json()
    records = data.get("records", [])
    return records[0] if records else None


def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a single Airtable record. Keys must match column names verbatim.
    Returns Airtable's created record payload.
    """
    payload = {"fields": fields}
    r = _request_with_retries("POST", _table_url(), json=payload)
    return r.json()


# ---- backwards-compat alias -------------------------------------------------

def insert_record(*args, **kwargs):
    """
    Compatibility alias so 'from airtable_client import insert_record' keeps working.
    """
    return insert_row(*args, **kwargs)
