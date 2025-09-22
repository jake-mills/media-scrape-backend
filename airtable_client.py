# airtable_client.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional
from pyairtable import Table
from pyairtable.formulas import match

API_KEY = os.environ["AIRTABLE_API_KEY"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]  # e.g. "Videos & Images"

_table = Table(API_KEY, BASE_ID, TABLE_NAME)

# ---- Airtable helpers aligned to your column names ----
def find_by_source_url(url: Optional[str]) -> Optional[Dict[str, Any]]:
    if not url:
        return None
    rec = _table.first(formula=match({"Source URL": url}))
    return rec

def insert_row(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Expecting fields to already use your exact Airtable column names.
    # Unknown keys will be ignored by Airtable.
    return _table.create({"fields": fields})
