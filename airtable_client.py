# airtable_client.py
from __future__ import annotations

import os
import time
import math
from typing import Dict, List, Optional, Iterable, Any
from urllib.parse import quote

import httpx


class AirtableConfigError(RuntimeError):
    pass


def _get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise AirtableConfigError(f"Required environment variable {name} is not set.")
    return v


def _chunked(items: Iterable[dict], size: int = 10) -> Iterable[List[dict]]:
    batch: List[dict] = []
    for it in items:
        batch.append(it)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class AirtableClient:
    """
    Minimal Airtable client tailored for this project.

    - De-dup by the 'Source_URL' field
    - Async-first API + safe sync wrappers
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_id: Optional[str] = None,
        table_name: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key or _get_env("AIRTABLE_API_KEY")
        self.base_id = base_id or _get_env("AIRTABLE_BASE_ID")
        self.table_name = table_name or _get_env("AIRTABLE_TABLE_NAME")
        # Table name must be URL-encoded exactly once
        self._table_enc = quote(self.table_name, safe="")
        self.base_url = f"https://api.airtable.com/v0/{self.base_id}/{self._table_enc}"
        self.timeout = timeout

    # -------------------------
    # HTTP helpers
    # -------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def _aget(self, url: str, params: Optional[dict] = None) -> httpx.Response:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            return await client.get(url, headers=self._headers(), params=params)

    async def _apost(self, url: str, json: dict) -> httpx.Response:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            return await client.post(url, headers=self._headers(), json=json)

    # -------------------------
    # Core Airtable operations
    # -------------------------

    async def exists_by_source_url(self, source_url: str) -> bool:
        """
        Returns True if a record with Source_URL == source_url exists.
        """
        if not source_url:
            return False

        # filterByFormula needs a single-quoted string. We escape quotes defensively.
        safe = source_url.replace("'", "\\'")
        formula = f"{{Source_URL}}='{safe}'"
        params = {"filterByFormula": formula, "maxRecords": 1}

        # Small retry on 429
        for attempt in range(3):
            r = await self._aget(self.base_url, params=params)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "1"))
                time.sleep(min(3, retry_after))
                continue
            r.raise_for_status()
            data = r.json()
            return bool(data.get("records"))
        # If we kept hitting 429s, assume not existing (fail-open) to avoid missing inserts.
        return False

    async def insert_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert up to 10 records (Airtable API limit per request).
        Returns the created records.
        """
        if not records:
            return []

        payload = {"records": [{"fields": r} for r in records], "typecast": True}

        # Small retry/backoff on 429
        backoff = 1.0
        for attempt in range(4):
            resp = await self._apost(self.base_url, json=payload)
            if resp.status_code == 429:
                time.sleep(min(3.0, backoff))
                backoff *= 1.6
                continue
            resp.raise_for_status()
            body = resp.json()
            return body.get("records", [])
        return []

    async def insert_unique(
        self, items: List[Dict[str, Any]], dedupe_field: str = "Source_URL"
    ) -> List[Dict[str, Any]]:
        """
        De-duplicate by `dedupe_field` before inserting.
        Each item is a dict of Airtable fields (e.g., {"Title": "...", "Provider":"...", "Source_URL":"..."}).
        Returns list of created records (as returned by Airtable).
        """
        to_create: List[Dict[str, Any]] = []
        for item in items:
            src = str(item.get(dedupe_field, "")).strip()
            if not src:
                # Don’t attempt to insert items without the dedupe field
                continue
            if not await self.exists_by_source_url(src):
                to_create.append(item)

        created: List[Dict[str, Any]] = []
        for batch in _chunked(to_create, size=10):
            created.extend(await self.insert_records(batch))
        return created

    # -------------------------
    # Sync wrappers (safe if code forgets "await")
    # -------------------------

    def exists_by_source_url_sync(self, source_url: str) -> bool:
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Rare case: running inside loop; run in a thread
            return loop.run_until_complete(self.exists_by_source_url(source_url))  # type: ignore
        else:
            return asyncio.run(self.exists_by_source_url(source_url))

    def insert_unique_sync(
        self, items: List[Dict[str, Any]], dedupe_field: str = "Source_URL"
    ) -> List[Dict[str, Any]]:
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            return loop.run_until_complete(self.insert_unique(items, dedupe_field))  # type: ignore
        else:
            return asyncio.run(self.insert_unique(items, dedupe_field))
