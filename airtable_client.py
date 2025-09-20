# airtable_client.py
import os
from airtable import Airtable
from typing import Optional, Dict, Any, List

# Load Airtable credentials from environment variables
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
    raise RuntimeError(
        "Airtable credentials missing. Ensure AIRTABLE_API_KEY, "
        "AIRTABLE_BASE_ID, and AIRTABLE_TABLE_NAME are set in Render environment."
    )

# Initialize Airtable client
airtable = Airtable(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_API_KEY)


def airtable_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a single record into Airtable.
    """
    try:
        inserted = airtable.insert(record)
        return {"success": True, "record": inserted}
    except Exception as e:
        return {"success": False, "error": str(e)}


def airtable_bulk_insert(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Insert multiple records into Airtable.
    """
    results = []
    for record in records:
        try:
            inserted = airtable.insert(record)
            results.append({"success": True, "record": inserted})
        except Exception as e:
            results.append({"success": False, "error": str(e)})
    return {"inserted": results}


def airtable_exists_by_source_url(source_url: str) -> bool:
    """
    Check if a record with a given Source_URL already exists.
    """
    try:
        match = airtable.match("Source_URL", source_url)
        return bool(match)
    except Exception:
        return False


def airtable_find_by_field(field: str, value: Any) -> Optional[Dict[str, Any]]:
    """
    Find a record by a specific field.
    """
    try:
        return airtable.match(field, value)
    except Exception:
        return None


def airtable_update(record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update a record in Airtable by ID.
    """
    try:
        updated = airtable.update(record_id, fields)
        return {"success": True, "record": updated}
    except Exception as e:
        return {"success": False, "error": str(e)}
