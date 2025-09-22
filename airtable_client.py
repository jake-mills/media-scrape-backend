# airtable_client.py
from __future__ import annotations

import os
import time
import json
import logging
from typing import Any, Dict, List, Optional, TypedDict
from urllib.parse import quote

import requests

log = logging.getLogger("airtable_client")
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ------------------------------------------------------------------------------
# Environment / Config
# ------------------------------------------------------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Videos & Images").strip()

if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
    raise RuntimeError(
        "Missing one or more env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME"
    )

API_ROOT = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(AIRTABLE_TABLE_NAME)}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

# ------------------------------------------------------------------------------
# Normalized provider-agnostic item
# (Every provider should emit items with these keys.)
# ------------------------------------------------------------------------------
class NormalizedItem(TypedDict, total=False):
    # REQUIRED for upsert
    source_url: str         # canonical landing page or asset URL
    title: str
    provider: str           # e.g., "Openverse", "YouTube", "Internet Archive"
    media_type: str         # "Images" or "Videos" (must match your Airtable values)

    # OPTIONAL (we map these to your exact columns)
    thumbnail_url: str
    published_date: str
    license: str            # we store this entire string into your "Copyright" column
    notes: str


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _escape_for_formula(val: str) -> str:
    """Escape single quotes for Airtable formula strings."""
    # Airtable uses single-quoted strings in filterByFormula; double single-quotes inside.
    return val.replace("'", "''")


def _sleep_backoff(attempt: int) -> None:
    # exponential-ish but capped
    time.sleep(min(0.25 * (2 ** attempt), 3.0))


def _http_check(resp: requests.Response) -> None:
    if resp.status_code >= 400:
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        log.error("Airtable HTTP %s: %s", resp.status_code, body)
        resp.raise_for_status()


# ------------------------------------------------------------------------------
# Read: find existing record by Source URL (exact match)
# ------------------------------------------------------------------------------
def find_by_source_url(source_url: str) -> Optional[str]:
    """
    Returns Airtable record id if a row exists with {Source URL} == source_url; else None.
    """
    if not source_url:
        return None

    formula = "{Source URL} = '" + _escape_for_formula(source_url) + "'"
    params = {"filterByFormula": formula, "maxRecords": 1}
    attempt = 0
    while True:
        attempt += 1
        resp = requests.get(API_ROOT, headers=HEADERS, params=params, timeout=30)
        if resp.status_code in (429, 500, 502, 503, 504) and attempt <= 5:
            _sleep_backoff(attempt)
            continue
        _http_check(resp)
        data = resp.json()
        records = data.get("records", [])
        if records:
            return records[0].get("id")
        return None


# ------------------------------------------------------------------------------
# Mapping to YOUR exact Airtable columns
# (Do NOT include "Index" — Airtable Autonumber handles it.)
# ------------------------------------------------------------------------------
def to_airtable_fields(
    item: NormalizedItem,
    *,
    run_id: str,
    search_topics_used: str,
    search_dates_used: str,
) -> Dict[str, Any]:
    """
    Build a field dict matching your Airtable table columns EXACTLY:
      - Index                 (Autonumber; never set here)
      - Media Type            (Images | Videos)
      - Provider
      - Thumbnail
      - Title
      - Source URL
      - Search Topics Used
      - Search Dates Used
      - Published/Created
      - Copyright
      - Run ID
      - Notes
    """
    return {
        "Media Type": item.get("media_type", ""),
        "Provider": item.get("provider", ""),
        "Thumbnail": item.get("thumbnail_url", ""),
        "Title": item.get("title", ""),
        "Source URL": item.get("source_url", ""),
        "Search Topics Used": search_topics_used,
        "Search Dates Used": search_dates_used,
        "Published/Created": item.get("published_date", ""),
        "Copyright": item.get("license", ""),
        "Run ID": run_id,
        "Notes": item.get("notes", ""),
    }


# ------------------------------------------------------------------------------
# Write: insert one row
# ------------------------------------------------------------------------------
def insert_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a single row. Returns Airtable's record JSON.
    Raises for HTTP errors; caller should catch if needed.
    """
    payload = {"fields": fields, "typecast": True}
    attempt = 0
    while True:
        attempt += 1
        resp = requests.post(API_ROOT, headers=HEADERS, data=json.dumps(payload), timeout=30)
        if resp.status_code in (429, 500, 502, 503, 504) and attempt <= 5:
            _sleep_backoff(attempt)
            continue
        _http_check(resp)
        return resp.json()


# ------------------------------------------------------------------------------
# Upsert many (de-dup by "Source URL"), honoring target_count
# ------------------------------------------------------------------------------
def upsert_items(
    items: List[NormalizedItem],
    *,
    run_id: str,
    search_topics_used: str,
    search_dates_used: str,
    target_count: int,
) -> Dict[str, Any]:
    """
    Insert up to target_count NEW rows into Airtable.
    Skips (does not update) rows whose {Source URL} already exists.
    Returns a summary: insertedCount, skippedCount, and (optionally) some echoes.
    """
    inserted = 0
    skipped = 0
    echoes: List[Dict[str, Any]] = []

    for it in items:
        if inserted >= target_count:
            break

        # Ensure minimal required fields exist
        src = (it.get("source_url") or "").strip()
        ttl = (it.get("title") or "").strip()
        prv = (it.get("provider") or "").strip()
        mtp = (it.get("media_type") or "").strip()
        if not (src and ttl and prv and mtp):
            skipped += 1
            continue

        # De-dupe by Source URL
        try:
            if find_by_source_url(src):
                skipped += 1
                continue
        except Exception as e:
            # If read fails, log and skip to be safe
            log.warning("find_by_source_url failed (%s); skipping item", e)
            skipped += 1
            continue

        # Map & insert
        fields = to_airtable_fields(
            it,
            run_id=run_id or "",
            search_topics_used=search_topics_used or "",
            search_dates_used=search_dates_used or "",
        )
        try:
            record = insert_row(fields)
            inserted += 1
            # Keep a light echo (first few) – helpful for Swagger response
            if len(echoes) < 5:
                echoes.append(
                    {
                        "id": record.get("id"),
                        "fields": record.get("fields", {}),
                        "source_url": src,
                    }
                )
        except Exception as e:
            log.exception("Insert failed for %s: %s", src, e)
            skipped += 1

    return {
        "insertedCount": inserted,
        "skippedCount": skipped,
        "inserted": echoes,
    }
