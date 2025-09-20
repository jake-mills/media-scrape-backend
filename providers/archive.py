import os, httpx
from typing import List, Dict, Any, Optional

ARCHIVE_ROWS = int(os.getenv("ARCHIVE_ROWS", "50"))

async def fetch_archive_async(topic: str, rows: int = ARCHIVE_ROWS,
                              year_start: Optional[int] = None,
                              year_end: Optional[int] = None) -> List[Dict[str, Any]]:
    # Compose query with mediatype and optional year range
    base = f'(title:("{topic}") OR description:("{topic}")) AND (mediatype:(movies) OR mediatype:(image))'
    if year_start and year_end:
        q = base + f' AND year:[{year_start} TO {year_end}]'
    elif year_start:
        q = base + f' AND year:[{year_start} TO {year_start}]'
    else:
        q = base
    params = {
        "q": q,
        "fl[]": ["identifier","title","mediatype","year"],
        "rows": min(rows, 100),
        "output": "json"
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get("https://archive.org/advancedsearch.php", params=params)
        r.raise_for_status()
        data = r.json()
    docs = data.get("response", {}).get("docs", [])
    out: List[Dict[str, Any]] = []
    for d in docs:
        try:
            ident = d.get("identifier")
            title = d.get("title") or ""
            mediatype = d.get("mediatype") or ""
            year = d.get("year") or ""
            url = f"https://archive.org/details/{ident}"
            thumb = f"https://archive.org/services/img/{ident}"
            mtype = "Video" if mediatype == "movies" else "Image" if mediatype == "image" else "Image"
            out.append({
                "title": title,
                "url": url,
                "thumbnail": thumb,
                "type": mtype,
                "provider": "Archive",
                "published_at": f"{year}-01-01" if str(year).isdigit() else "",
                "copyright": "Archive"
            })
        except Exception:
            continue
    return out
