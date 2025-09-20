from __future__ import annotations
from datetime import datetime
import re
from typing import Optional, Tuple

def parse_search_dates(s: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Accepts inputs like:
      - "1970-1985"
      - "1970–1985" (en-dash)
      - "1970"
      - "1970-01-01 to 1985-12-31"
      - ISO date single: "1970-06-01"
    Returns (published_after_iso, published_before_iso) suitable for YouTube.
      - For a single year YYYY => (YYYY-01-01T00:00:00Z, YYYY-12-31T23:59:59Z)
      - For range yyyy..yyyy => corresponding endpoints
      - If cannot parse => (None, None)
    """
    if not s:
        return (None, None)
    s = s.strip()

    # Try ISO single date
    try:
        dt = datetime.fromisoformat(s.replace("Z",""))
        start = dt.strftime("%Y-%m-%dT00:00:00Z")
        end = dt.strftime("%Y-%m-%dT23:59:59Z")
        return (start, end)
    except Exception:
        pass

    # Replace " to " with hyphen
    s = s.replace(" to ", "-")
    s = s.replace("–", "-")  # en-dash to hyphen

    # Year range YYYY-YYYY
    m = re.match(r"^\s*(\d{4})\s*-\s*(\d{4})\s*$", s)
    if m:
        y1 = int(m.group(1)); y2 = int(m.group(2))
        if y1 > y2: y1, y2 = y2, y1
        start = f"{y1}-01-01T00:00:00Z"
        end   = f"{y2}-12-31T23:59:59Z"
        return (start, end)

    # Single year YYYY
    m = re.match(r"^\s*(\d{4})\s*$", s)
    if m:
        y = int(m.group(1))
        start = f"{y}-01-01T00:00:00Z"
        end   = f"{y}-12-31T23:59:59Z"
        return (start, end)

    # Unparsed
    return (None, None)

def archive_year_bounds(s: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Return (start_year, end_year) for Archive filter hints.
    """
    a,b = parse_search_dates(s)
    def year_of(iso: Optional[str]) -> Optional[int]:
        try:
            return int(iso[:4]) if iso else None
        except Exception:
            return None
    return (year_of(a), year_of(b))
