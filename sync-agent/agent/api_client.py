# sync-agent/agent/api_client.py
from datetime import datetime
import gzip
import httpx

class ApiClient:
    def __init__(self, api_url, api_key, http: httpx.Client | None = None):
        self.api_url = api_url.rstrip("/")
        self._http = http or httpx.Client(base_url=self.api_url, timeout=60.0)
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def get_highwater(self) -> datetime | None:
        r = self._http.get(f"{self.api_url}/v1/highwater", headers=self._headers)
        r.raise_for_status()
        hw = r.json().get("highwater")
        return datetime.fromisoformat(hw) if hw else None

    def post_ingest(self, lp: bytes) -> None:
        body = gzip.compress(lp)
        headers = dict(self._headers)
        headers["Content-Type"] = "text/plain; charset=utf-8"
        headers["Content-Encoding"] = "gzip"
        r = self._http.post(f"{self.api_url}/v1/ingest", content=body, headers=headers)
        r.raise_for_status()
