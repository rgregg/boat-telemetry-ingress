from datetime import datetime, timezone

H = {"Authorization": "Bearer key-a"}

def test_ingest_requires_auth(client):
    assert client.post("/v1/ingest", content=b"cpu f=1 1").status_code == 401

def test_ingest_bad_key(client):
    r = client.post("/v1/ingest", content=b"cpu f=1 1", headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401

def test_ingest_writes_with_injected_tags(client, fake_influx):
    r = client.post("/v1/ingest", content=b"cpu load=1 100", headers=H)
    assert r.status_code == 200 and r.json() == {"written": True}
    bucket, lp, _ = fake_influx.writes[0]
    assert bucket == "bucket_a"
    assert lp == b"cpu,source=site_a load=1 100"

def test_ingest_rejects_empty_body(client):
    assert client.post("/v1/ingest", content=b"", headers=H).status_code == 400

def test_ingest_influx_failure_returns_502(client, fake_influx):
    fake_influx.fail_write = True
    assert client.post("/v1/ingest", content=b"cpu f=1 1", headers=H).status_code == 502

def test_highwater_returns_iso_or_null(client, fake_influx):
    assert client.get("/v1/highwater", headers=H).json() == {"highwater": None}
    fake_influx.hw = datetime(2026, 6, 14, 18, 40, tzinfo=timezone.utc)
    assert client.get("/v1/highwater", headers=H).json() == {"highwater": "2026-06-14T18:40:00+00:00"}

def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["influxdb"] is True

def test_ingest_gzip_body(client, fake_influx):
    import gzip
    body = gzip.compress(b"cpu load=1 100")
    r = client.post("/v1/ingest", content=body, headers={**H, "Content-Encoding": "gzip"})
    assert r.status_code == 200
    bucket, lp, _ = fake_influx.writes[0]
    assert lp == b"cpu,source=site_a load=1 100"

def test_ingest_malformed_gzip_returns_400(client):
    r = client.post("/v1/ingest", content=b"not-gzip", headers={**H, "Content-Encoding": "gzip"})
    assert r.status_code == 400

def test_ingest_non_utf8_body_returns_400(client):
    r = client.post("/v1/ingest", content=b"\xff\xfe", headers=H)
    assert r.status_code == 400

def test_ingest_influxdb_4xx_returns_400(client, fake_influx):
    fake_influx.raise_status = 422  # InfluxDB field-type conflict / unparseable
    r = client.post("/v1/ingest", content=b"cpu f=1 1", headers=H)
    assert r.status_code == 400

def test_ingest_influxdb_5xx_returns_502(client, fake_influx):
    fake_influx.raise_status = 503
    r = client.post("/v1/ingest", content=b"cpu f=1 1", headers=H)
    assert r.status_code == 502
