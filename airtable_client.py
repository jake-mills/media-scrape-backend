from __future__ import annotations

import os
import urllib.parse
from typing import List, Dict, Any, Tuple

import httpx


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


DEBUG = _bool_env("DEBUG_OPENVERSE", False)


class AirtableClient:
    """
    Minimal async Airtable client for a single table.

    Fields we write:
      - Title (str)
      - Provider (str)
      - Source_URL (str)

    We de-dup on Source_URL using filterByFormula.
    """

    API_BASE = "https://api.airtable.com/v0"

    def __init__(self, api_key: str, base_id: str, table_name: str) -> None:
        if not api_key or not base_id or not table_name:
            raise ValueError("AirtableClient requires api_key, base_id, and table_name")

        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        self.base_url = f"{self.API_BASE}/{self.base_id}/{urllib.parse.quote(self.table_name)}"
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    # -----------------------------
    # helpers
    # -----------------------------
    @staticmethod
    def _formula_equals(field: str, value: str) -> str:
        """
        Build a filterByFormula that matches an exact string (quotes escaped).
        Example: {Source_URL} = 'https://example.com/a?b=1'
        """
        # Escape single quotes per Airtable formula rules
        safe = value.replace("'", "\\'")
        return f"{{{field}}} = '{safe}'"

    # -----------------------------
    # public API
    # -----------------------------
    async def exists_by_source_url(self, url: str) -> bool:
        """
        Return True if a record already exists with Source_URL == url.
        """
        params = {
            "filterByFormula": self._formula_equals("Source_URL", url),
            "pageSize": 1,
            "fields[]": ["Source_URL"],
        }
        if DEBUG:
            print(f"[Airtable] EXISTS check: params={params}")

        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(self.base_url, headers=self._headers, params=params)
            if DEBUG:
                print(f"[Airtable] EXISTS status={r.status_code} body={r.text[:400]}")
            r.raise_for_status()
            data = r.json()
            return bool(data.get("records"))

    async def insert_rows(self, rows: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Bulk insert rows shaped like:
            {"Title": str, "Provider": str, "Source_URL": str}

        Returns (inserted_count, inserted_rows_echo).
        We do not upsert here; call exists_by_source_url() first if you want to de-dup.
        """
        if not rows:
            return 0, []

        payload = {
            "records": [{"fields": row} for row in rows],
            "typecast": True,  # let Airtable coerce basic types where possible
        }
        if DEBUG:
            echo = [{k: (v if k != "Source_URL" else v[:120]) for k, v in r.items()} for r in rows]
            print(f"[Airtable] INSERT {len(rows)} rows: {echo}")

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(self.base_url, headers=self._headers, json=payload)
            if DEBUG:
                print(f"[Airtable] INSERT status={r.status_code} body={r.text[:500]}")
            r.raise_for_status()
            data = r.json()
            created = data.get("records", [])
            # Echo back the rows we attempted to create (fields only)
            return len(created), rows