# Media Scrape Backend (Cloud, Option C+ with Prefilled Base ID + Date Filters)
FastAPI backend for your Apple Shortcut. Parallel provider fetch (YouTube, Openverse, Internet Archive), URL normalization, Airtable dedupe, **batch insert**, auth header, and Render/Railway blueprints.

This build includes:
- `AIRTABLE_BASE_ID` default set to **app7iv7XirA2VzppE** (your base)
- Safety checks for missing `AIRTABLE_API_KEY`
- Date-range parsing from **"Search Dates Used"** (YYYY or YYYY–YYYY or ISO dates)
  - YouTube: `publishedAfter/publishedBefore`
  - Archive: `year:[YYYY TO YYYY]` (approx via query composition)

See `README` sections below for deployment and Shortcut instructions.
