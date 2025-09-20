# airtable_client.py
"""
Airtable client wrapper using pyairtable.
Centralizes connection and common operations for the Media Scrape backend.
"""

import os
from typing import Optional, Dict, Any, List
from pyairtable import Table

# Load environment variables
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")


def get_table() -> Table:
    """
    Returns a configured Airtable Table instance.
    Raises RuntimeError if required environment variables are missing.
    """
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
        raise RuntimeError("Missing Airtable environment variables: "
                           "AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME")
    return Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)


def insert_row(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a single record into Airtable.
    """
    table = get_table()
    return table.create(record)


def insert_rows(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Insert multiple records into Airtable.
    """
    table = get_table()
    return table.batch_create(records)


def find_by_source_url(url: str, field: str = "Source_URL") -> Optional[Dict[str, Any]]:
    """
    Find a record by its Source_URL (or another field if specified).
    Returns the first match or None if no match found.
    """
    table = get_table()
    formula = f"{{{field}}} = '{url}'"
    matches = table.all(formula=formula, max_records=1)
    return matches[0] if matches else None


def update_row(record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing record by ID.
    """
    table = get_table()
    return table.update(record_id, fields)


def delete_row(record_id: str) -> Dict[str, Any]:
    """
    Delete a record by ID.
    """
    table = get_table()
    return table.delete(record_id)
