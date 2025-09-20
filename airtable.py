import os, asyncio, json
from typing import List, Dict, Any
from urllib.parse import quote
import httpx

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
if not AIRTABLE_API_KEY:
    raise RuntimeError("AIRTABLE_API_KEY missing. Set it in your environment.")

AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "app7iv7XirA2VzppE")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Videos & Images")

def _table_url() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(AIRTABLE_TABLE_NAME, safe='')}"

_headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

async def airtable_exists_by_source_url(source_url: str) -> bool:
    formula = f'{{Source URL}}="{source_url}"'
    url = _table_url() + f"?filterByFormula={quote(formula, safe='')}"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=_headers)
        r.raise_for_status()
        data = r.json()
        return len(data.get("records", [])) > 0

async def airtable_batch_create(records_fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    created: List[Dict[str, Any]] = []
    if not records_fields:
        return created
    chunks = [records_fields[i:i+10] for i in range(0, len(records_fields), 10)]
    async with httpx.AsyncClient(timeout=60) as client:
        for chunk in chunks:
            payload = {"records": [{"fields": f} for f in chunk]}
            try:
                r = await client.post(_table_url(), headers=_headers, content=json.dumps(payload))
                if r.status_code == 429:
                    await asyncio.sleep(1.5)
                    r = await client.post(_table_url(), headers=_headers, content=json.dumps(payload))
                r.raise_for_status()
                data = r.json()
                for rec in data.get("records", []):
                    created.append(rec.get("fields", {}))
            except Exception:
                created.extend(chunk)
    return created
