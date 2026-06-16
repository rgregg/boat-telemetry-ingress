# sync-agent/tests/test_local_influx.py
import json
from datetime import datetime, timezone
import httpx
from agent.local_influx import LocalInflux

CSV = (
    "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string\r\n"
    ",result,table,_start,_stop,_time,_value,_field,_measurement,host\r\n"
    ",_result,0,,,2026-06-14T18:40:00Z,1.5,load,cpu,a\r\n"
)

def test_query_window_builds_flux_and_returns_lp():
    seen = {}
    def handler(req):
        seen["url"] = str(req.url); seen["body"] = req.content.decode()
        seen["auth"] = req.headers.get("authorization"); seen["ct"] = req.headers.get("content-type")
        return httpx.Response(200, text=CSV)
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://localhost:8086")
    li = LocalInflux("http://localhost:8086", "tok", "org", "mybucket", http=http)
    start = datetime(2026, 6, 14, 18, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 6, 14, 19, 0, tzinfo=timezone.utc)
    lp = li.query_window(start, stop)
    assert lp == "cpu,host=a load=1.5 1781462400000000000"
    assert "/api/v2/query" in seen["url"] and "org=org" in seen["url"]
    # Body is JSON requesting the datatype annotation (so field types serialize correctly).
    body = json.loads(seen["body"])
    assert 'from(bucket:"mybucket")' in body["query"]
    assert "2026-06-14T18:00:00+00:00" in body["query"]
    assert "2026-06-14T19:00:00+00:00" in body["query"]
    assert "datatype" in body["dialect"]["annotations"]
    assert seen["ct"] == "application/json"
    assert seen["auth"] == "Token tok"

def test_query_window_quotes_string_fields():
    # A string _value (per #datatype) must come out quoted — the bug that broke
    # the real boat data when annotations were missing.
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,POWER_ON_5V_ON,value,electrical.sailorhat.state\r\n"
    )
    http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, text=csv)),
                        base_url="http://localhost:8086")
    li = LocalInflux("http://localhost:8086", "tok", "org", "b", http=http)
    s = datetime(2026, 6, 14, tzinfo=timezone.utc)
    assert li.query_window(s, s) == 'electrical.sailorhat.state value="POWER_ON_5V_ON" 1781462400000000000'

def test_query_window_empty_returns_empty():
    http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, text="\r\n")),
                        base_url="http://localhost:8086")
    li = LocalInflux("http://localhost:8086", "tok", "org", "b", http=http)
    s = datetime(2026, 6, 14, tzinfo=timezone.utc)
    assert li.query_window(s, s) == ""
