# ingress-api/tests/test_influx.py
import httpx, pytest
from app.influx import InfluxClient

def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://influx:8086")

def test_write_posts_lp_to_bucket():
    seen = {}
    def handler(req):
        seen["url"] = str(req.url); seen["body"] = req.content; seen["auth"] = req.headers.get("authorization")
        return httpx.Response(204)
    ic = InfluxClient("http://influx:8086", "tok", "org", http=_client(handler))
    ic.write("bucket_a", b"cpu load=1 1")
    assert "/api/v2/write" in seen["url"]
    assert "bucket=bucket_a" in seen["url"] and "org=org" in seen["url"]
    assert seen["auth"] == "Token tok"
    assert seen["body"] == b"cpu load=1 1"

def test_write_raises_on_error_status():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(500, text="boom")))
    with pytest.raises(httpx.HTTPStatusError):
        ic.write("b", b"x f=1 1")

def test_highwater_parses_latest_time():
    csv = ("#datatype,string,long,dateTime:RFC3339\n"
           ",result,table,_time\n"
           ",_result,0,2026-06-14T18:40:00Z\n")
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(200, text=csv)))
    hw = ic.highwater("bucket_a")
    assert hw is not None and hw.isoformat() == "2026-06-14T18:40:00+00:00"

def test_highwater_returns_none_when_empty():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(200, text="\r\n")))
    assert ic.highwater("bucket_a") is None

def test_health_true_on_pass():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(200, json={"status": "pass"})))
    assert ic.health() is True

def test_health_false_on_failure():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(503)))
    assert ic.health() is False

def test_write_sends_gzip_encoding_when_flagged():
    seen = {}
    def handler(req):
        seen["enc"] = req.headers.get("content-encoding")
        return httpx.Response(204)
    ic = InfluxClient("http://influx:8086", "tok", "org", http=_client(handler))
    ic.write("b", b"x f=1 1", gzipped=True)
    assert seen["enc"] == "gzip"

def test_health_false_when_status_is_fail():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(200, json={"status": "fail"})))
    assert ic.health() is False

def test_health_false_on_non_json_body():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(200, text="OK")))
    assert ic.health() is False

def test_highwater_raises_on_error_status():
    ic = InfluxClient("http://influx:8086", "tok", "org",
                      http=_client(lambda r: httpx.Response(500)))
    with pytest.raises(httpx.HTTPStatusError):
        ic.highwater("b")
