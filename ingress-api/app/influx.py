# ingress-api/app/influx.py
from datetime import datetime
import httpx

class InfluxClient:
    def __init__(self, url: str, token: str, org: str, http: httpx.Client | None = None):
        self.url = url.rstrip("/")
        self.org = org
        self._http = http or httpx.Client(base_url=self.url, timeout=30.0)
        self._headers = {"Authorization": f"Token {token}"}

    def write(self, bucket: str, lp: bytes, gzipped: bool = False) -> None:
        headers = dict(self._headers)
        headers["Content-Type"] = "text/plain; charset=utf-8"
        if gzipped:
            headers["Content-Encoding"] = "gzip"
        r = self._http.post(
            f"{self.url}/api/v2/write",
            params={"org": self.org, "bucket": bucket, "precision": "ns"},
            content=lp, headers=headers,
        )
        r.raise_for_status()

    def highwater(self, bucket: str) -> datetime | None:
        flux = (f'from(bucket:"{bucket}") |> range(start: 0) '
                '|> keep(columns:["_time"]) '
                '|> sort(columns:["_time"], desc:true) |> limit(n:1)')
        headers = dict(self._headers)
        headers["Content-Type"] = "application/vnd.flux"
        headers["Accept"] = "application/csv"
        r = self._http.post(f"{self.url}/api/v2/query", params={"org": self.org},
                            content=flux, headers=headers)
        r.raise_for_status()
        return _last_time_from_csv(r.text)

    def health(self) -> bool:
        try:
            r = self._http.get(f"{self.url}/health")
        except httpx.HTTPError:
            return False
        return r.status_code == 200 and r.json().get("status") == "pass"

def _last_time_from_csv(text: str) -> datetime | None:
    time_col = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        cells = line.split(",")
        if line.startswith("#"):
            continue
        if "_time" in cells:               # header row
            time_col = cells.index("_time")
            continue
        if time_col is not None and len(cells) > time_col:
            val = cells[time_col].strip()
            if val:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
    return None
