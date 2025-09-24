import os
import asyncio
from typing import Dict, Any, Optional
import httpx

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "").strip()

_API = "https://api.airtable.com/v0"

def _auth_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

def _table_url() -> str:
    return f"{_API}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"

async def _retryable_get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(3):
            resp = await client.get(url, headers=_auth_headers(), params=params)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"Airtable GET failed {resp.status_code}: {resp.text}")
    return None

async def _retryable_post(url: str, json: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(3):
            resp = await client.post(url, headers={**_auth_headers(),"Content-Type":"application/json"}, json=json)
            if resp.status_code in (200, 201):
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"Airtable POST failed {resp.status_code}: {resp.text}")
    raise RuntimeError("Airtable POST exhausted retries")

async def find_by_source_url(url: str) -> bool:
    if not url:
        return False
    params = {
        "filterByFormula": f"{{Source URL}}='{url.replace(\"'\",\"\\'\")}'",
        "maxRecords": 1,
    }
    data = await _retryable_get(_table_url(), params)
    records = (data or {}).get("records", [])
    return len(records) > 0

async def insert_record(fields: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"records": [{"fields": fields}], "typecast": True}
    return await _retryable_post(_table_url(), payload)
