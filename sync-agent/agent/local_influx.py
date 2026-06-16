# sync-agent/agent/local_influx.py
import json
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
            "Content-Type": "application/json",
            "Accept": "application/csv",
        }

    def query_window(self, start: datetime, stop: datetime) -> str:
        flux = (f'from(bucket:"{self.bucket}") '
                f'|> range(start: {start.isoformat()}, stop: {stop.isoformat()})')
        # Request the `datatype` annotation so field values are serialized with the
        # correct type (strings/booleans quoted, ints suffixed). A raw vnd.flux
        # POST returns CSV WITHOUT annotations, which would make every field
        # serialize as a bare number and produce invalid line protocol for any
        # string/boolean/datetime field.
        body = {"query": flux, "dialect": {"annotations": ["datatype"], "header": True}}
        r = self._http.post(f"{self.url}/api/v2/query", params={"org": self.org},
                            content=json.dumps(body), headers=self._headers)
        r.raise_for_status()
        return annotated_csv_to_lp(r.text)
