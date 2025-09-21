# airtable_client.py
from __future__ import annotations

import os
import time
import json
from typing import Dict, Any, Optional

import requests

# --- Config ---------------------------------------------------------------

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Videos & Images")

# IMPORTANT: your column is literally "Source URL" (with a space)
SOURCE_FIELD = "Source URL"
TITLE_FIELD = "Title"
PROVIDER_FIELD = "Provider"

API_ROOT = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
SESSION = requests.Session()
SESSION.headers.update(
    {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }
)

# --- Errors / helpers -----------------------------------------------------

class AirtableError(Exception):
    def __init__(self, message: str, status: int, payload: Dict[str, Any], response_json: Optional[Dict[str, Any]]):
        super().__init__(message)
        self.status = status
        self.payload = payload
        self.response_json = response_json

def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(2 ** attempt * 0.25, 3.0))

def _safe_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a minimal, schema-aligned field dict using your exact column names:
      - Title        (text)
      - Provider     (text)
      - Source URL   (URL or text)
    """
    title = raw.get("title") or raw.get("Title") or ""
    provider = raw.get("provider") or raw.get("Provider") or ""
    source = (
        raw.get("source_url")
        or raw.get("Source_URL")
        or raw.get("Source URL")
        or raw.get("url")
        or raw.get("permalink")
        or raw.get("source")
        or ""
    )
    return {
        TITLE_FIELD: title[:500],
        PROVIDER_FIELD: provider[:100],
        SOURCE_FIELD: source,
    }

# --- Public API -----------------------------------------------------------

def find_by_source_url(source_url: str) -> Optional[str]:
    """
    Returns record id if a row with {Source URL} == source_url exists, else None.
    """
    if not source_url:
        return None

    url = f"{API_ROOT}/{requests.utils.quote(AIRTABLE_TABLE_NAME, safe='')}"
    # Escape single quotes in value for Airtable formula
    safe_val = source_url.replace("'", "\\'")
    params = {
        "filterByFormula": f"{{{SOURCE_FIELD}}}='{safe_val}'",
        "maxRecords": 1,
    }

    try:
        resp = SESSION.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        recs = data.get("records", [])
        return recs[0].get("id") if recs else None
    except requests.HTTPError:
        # On read hiccups, just treat as not found (avoid hard failures during runs)
        return None


def insert_row(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inserts one row using Title/Provider/Source URL.
    Surfaces Airtable's error body if validation fails (422).
    """
    url = f"{API_ROOT}/{requests.utils.quote(AIRTABLE_TABLE_NAME, safe='')}"
    fields = _safe_fields(raw_item)
    payload = {"records": [{"fields": fields}], "typecast": True}

    for attempt in range(4):
        try:
            resp = SESSION.post(url, data=json.dumps(payload), timeout=25)
            if resp.status_code >= 400:
                try:
                    body = resp.json()
                except Exception:
                    body = None
                raise AirtableError(
                    message=f"Airtable insert failed (status={resp.status_code})",
                    status=resp.status_code,
                    payload=payload,
                    response_json=body,
                )
            data = resp.json()
            return {
                "status": "inserted",
                "id": data.get("records", [{}])[0].get("id"),
                "fields": fields,
            }
        except AirtableError:
            # 4xx other than 429 = validation; do not retry
            raise
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (429, 500, 502, 503, 504):
                _sleep_backoff(attempt)
                continue
            raise
        except (requests.ConnectionError, requests.Timeout):
            _sleep_backoff(attempt)
            continue

    raise AirtableError(
        message="Airtable insert failed after retries",
        status=0,
        payload=payload,
        response_json=None,
    )
