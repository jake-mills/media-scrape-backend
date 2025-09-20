# airtable_client.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from pyairtable import Table
from pyairtable.formulas import match

# Field name we use to detect duplicates
SOURCE_URL_FIELD = "Source_URL"


class AirtableClient:
    """
    Minimal Airtable client for this service.
    - De-duplicate rows by Source_URL
    - Batch insert new rows (Airtable bulk limit = 10)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_id: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> None:
        api_key = api_key or os.environ["AIRTABLE_API_KEY"]
        base_id = base_id or os.environ["AIRTABLE_BASE_ID"]
        table_name = table_name or os.environ["AIRTABLE_TABLE_NAME"]

        # pyairtable Table client
        # Docs: https://pyairtable.readthedocs.io/en/stable/getting_started.html
        self.table = Table(api_key, base_id, table_name)

    # ---------- lookups ----------

    def exists_by_source_url(self, source_url: str) -> bool:
        """
        Return True if a record with the given Source_URL already exists.
        Uses first(formula=match({...})) per pyairtable docs.
        """
        if not source_url:
            return False
        rec = self.table.first(formula=match({SOURCE_URL_FIELD: source_url}))
        return rec is not None

    # ---------- inserts ----------

    def insert_many(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert new items that do not already exist by Source_URL.
        Each item can be either a plain dict of fields OR {"fields": {...}}.
        Returns the list of created records (id + fields) from Airtable.
        """
        # Normalize to plain field dicts
        normalized: List[Dict[str, Any]] = []
        for item in items:
            fields = item.get("fields", item)
            # Keep only non-empty dicts
            if isinstance(fields, dict) and fields:
                normalized.append(fields)

        # Filter out anything missing Source_URL or already present
        to_create: List[Dict[str, Any]] = []
        for fields in normalized:
            src = fields.get(SOURCE_URL_FIELD) or fields.get(SOURCE_URL_FIELD.lower())
            if not src:
                continue
            if not self.exists_by_source_url(str(src)):
                to_create.append(fields)

        if not to_create:
            return []

        # Airtable bulk create limit is 10 per request -> chunk
        created: List[Dict[str, Any]] = []
        for i in range(0, len(to_create), 10):
            chunk = to_create[i : i + 10]
            # Docs: table.batch_create([...])
            created.extend(self.table.batch_create(chunk))
        return created
