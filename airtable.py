# airtable_client.py
import aiohttp
from aiohttp import ClientTimeout


class AirtableClient:
    """
    Minimal async Airtable wrapper for a single table.
    Requires the table to contain fields:
      - Title (Text)
      - Provider (Text)
      - Source_URL (URL/Text)
    """

    def __init__(self, base_id: str, table_name: str, api_key: str, session: aiohttp.ClientSession | None = None):
        self.base_id = base_id
        self.table_name = table_name
        self.api_key = api_key

        self._session = session
        self._owns_session = session is None

        self.base_url = f"https://api.airtable.com/v0/{self.base_id}/{self.table_name}"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=ClientTimeout(total=20))
        return self._session

    async def close(self):
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def exists_by_source_url(self, source_url: str) -> bool:
        """
        Returns True if a record already exists with the same Source_URL.
        """
        safe = source_url.replace("'", r"\'")  # escape single quotes for formula
        params = {
            "filterByFormula": f"{{Source_URL}} = '{safe}'",
            "maxRecords": 1,
        }
        async with self.session.get(self.base_url, headers=self.headers, params=params) as resp:
            data = await resp.json()
            return bool(data.get("records"))

    async def insert_record(self, fields: dict) -> dict:
        payload = {"records": [{"fields": fields}]}
        async with self.session.post(self.base_url, headers=self.headers, json=payload) as resp:
            resp.raise_for_status()
            return await resp.json()
