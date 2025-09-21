import os
import requests
import urllib.parse

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Videos & Images")

# ✅ Ensure table name is URL-encoded (spaces, &, etc.)
ENCODED_TABLE_NAME = urllib.parse.quote(AIRTABLE_TABLE_NAME)

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ENCODED_TABLE_NAME}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}


def insert_row(record: dict):
    """Insert a new row into Airtable."""
    response = requests.post(
        AIRTABLE_URL,
        headers=HEADERS,
        json={"fields": record}
    )
    response.raise_for_status()
    return response.json()


def find_by_source_url(url: str):
    """Find rows in Airtable by source_url field."""
    formula = f"{{source_url}}='{url}'"
    response = requests.get(
        AIRTABLE_URL,
        headers=HEADERS,
        params={"filterByFormula": formula}
    )
    response.raise_for_status()
    return response.json().get("records", [])
