import os
from typing import Optional, Dict, Any
from pyairtable import Table

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Media")

# Fail fast if the three required env vars aren’t present.
if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
    missing = [k for k, v in {
        "AIRTABLE_API_KEY": AIRTABLE_API_KEY,
        "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
        "AIRTABLE_TABLE_NAME": AIRTABLE_TABLE_NAME,
    }.items() if not v]
    raise RuntimeError(f"Missing Airtable env vars: {', '.join(missing)}")

_table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)


def _escape_for_formula(value: str) -> str:
    """
    Airtable formulas use single-quoted strings; single quotes must be escaped as \'
    """
    return value.replace("'", r"\'")


def find_by_source_url(source_url: str) -> Optional[Dict[str, Any]]:
    """
    Return the first record whose {Source_URL} exactly matches source_url, or None.
    """
    # Build a safe formula without nested f-string gymnastics.
    safe = _escape_for_formula(source_url)
    formula = "{Source_URL} = '" + safe + "'"

    # select() is limited to 1 record for speed.
    records = _table.iterate(formula=formula, page_size=1, max_records=1)
    for page in records:
        for rec in page:
            return rec  # first match
    return None


def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a record with the given fields; returns the Airtable record dict.
    """
    return _table.create(fields)
