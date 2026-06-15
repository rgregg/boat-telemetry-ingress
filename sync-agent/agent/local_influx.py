# sync-agent/agent/local_influx.py
from datetime import datetime
import httpx
from agent.csvlp import annotated_csv_to_lp

class LocalInflux:
    def __init__(self, url, token, org, bucket, http: httpx.Client | None = None):
        self.url = url.rstrip("/")
        self.org = org
        self.bucket = bucket
        self._http = http or httpx.Client(base_url=self.url, timeout=60.0)
        self._headers = {
            "Authorization": f"Token {token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        }

    def query_window(self, start: datetime, stop: datetime) -> str:
        flux = (f'from(bucket:"{self.bucket}") '
                f'|> range(start: {start.isoformat()}, stop: {stop.isoformat()})')
        r = self._http.post(f"{self.url}/api/v2/query", params={"org": self.org},
                            content=flux, headers=self._headers)
        r.raise_for_status()
        return annotated_csv_to_lp(r.text)
