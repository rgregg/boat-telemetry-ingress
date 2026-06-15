# sync-agent/tests/test_api_client.py
from datetime import datetime, timezone
import gzip, httpx, pytest
from agent.api_client import ApiClient

def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://api:8080")

def test_get_highwater_parses_iso():
    api = ApiClient("http://api:8080", "k",
                    http=_client(lambda r: httpx.Response(200, json={"highwater": "2026-06-14T18:40:00+00:00"})))
    assert api.get_highwater() == datetime(2026, 6, 14, 18, 40, tzinfo=timezone.utc)

def test_get_highwater_null_returns_none():
    api = ApiClient("http://api:8080", "k",
                    http=_client(lambda r: httpx.Response(200, json={"highwater": None})))
    assert api.get_highwater() is None

def test_post_ingest_sends_gzip_and_bearer():
    seen = {}
    def handler(req):
        seen["enc"] = req.headers.get("content-encoding")
        seen["auth"] = req.headers.get("authorization")
        seen["body"] = gzip.decompress(req.content)
        return httpx.Response(200, json={"written": True})
    api = ApiClient("http://api:8080", "k", http=_client(handler))
    api.post_ingest(b"cpu load=1 1")
    assert seen["enc"] == "gzip"
    assert seen["auth"] == "Bearer k"
    assert seen["body"] == b"cpu load=1 1"

def test_post_ingest_raises_on_5xx():
    api = ApiClient("http://api:8080", "k", http=_client(lambda r: httpx.Response(502)))
    with pytest.raises(httpx.HTTPStatusError):
        api.post_ingest(b"cpu load=1 1")
