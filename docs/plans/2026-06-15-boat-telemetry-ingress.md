# Boat Telemetry Ingress — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable, no-loss telemetry relay — a central `ingress-api` container that owns the InfluxDB connection plus a stateless `sync-agent` container per remote site that forwards local InfluxDB data to it.

**Architecture:** The site-local InfluxDB is the durable buffer. Each cycle the agent asks the API for the per-source high-water mark, queries local InfluxDB forward in bounded windows, and POSTs line protocol. The API authenticates the source by API key, applies configured default tags, and writes to the source's bucket. The watermark only advances when data is confirmed written, so outages self-heal by backfill.

**Tech Stack:** Python 3.12 (alpine), FastAPI + uvicorn (API), httpx for all HTTP including InfluxDB's raw `/api/v2/write` and `/api/v2/query`, PyYAML for the source registry, pytest with `httpx.MockTransport` for fast hermetic tests. Docker Compose per component; deployed as Komodo stacks.

**Conventions:** Matches the home-lab `camera-health-monitor` service (single small app, env-var config, non-root container, `build: .` compose). Minimal pinned deps. Constant-time key comparison. UTC timezone-aware datetimes throughout. `now` is injected into core functions for testability.

**Key invariants (do not violate):**
- Each source maps **1:1 to its own bucket**. High-water = max `_time` over that bucket (no tag filter needed; tolerates pre-existing untagged data during cutover).
- The agent **never advances** past data the API has not confirmed (2xx). On any error it stops the cycle and retries next time; because it re-reads the watermark from the API each cycle, it resumes exactly where it left off.
- InfluxDB writes are idempotent on `(measurement, tag set, field, timestamp)`, so re-sending the `OVERLAP` window is harmless.

---

## File Structure

```
ingress-api/
  app/__init__.py
  app/config.py          # env settings loader
  app/registry.py        # Source dataclass + SourceRegistry (load yaml, authenticate)
  app/lp.py              # inject default tags into line-protocol lines
  app/influx.py          # InfluxClient: write / highwater / health (raw httpx)
  app/main.py            # FastAPI app: /v1/highwater, /v1/ingest, /health, auth dep
  tests/conftest.py
  tests/test_config.py
  tests/test_registry.py
  tests/test_lp.py
  tests/test_influx.py
  tests/test_main.py
  requirements.txt
  pytest.ini
  Dockerfile
  docker-compose.yml
  sources.example.yaml
  .env.example
  README.md

sync-agent/
  agent/__init__.py
  agent/config.py        # env settings loader
  agent/csvlp.py         # parse annotated CSV -> records -> line protocol
  agent/local_influx.py  # LocalInflux.query_window -> line protocol
  agent/api_client.py    # ApiClient.get_highwater / post_ingest
  agent/sync.py          # run_once: highwater -> windows -> post -> advance
  agent/main.py          # loop entry
  tests/conftest.py
  tests/test_config.py
  tests/test_csvlp.py
  tests/test_local_influx.py
  tests/test_api_client.py
  tests/test_sync.py
  requirements.txt
  pytest.ini
  Dockerfile
  docker-compose.yml
  .env.example
  README.md

docker-compose.e2e.yml   # repo root: influxdb + api + agent for end-to-end test
tests/e2e/test_e2e.py    # repo root end-to-end test
docs/runbooks/cutover.md
```

---

## Task 0: Scaffold both components

**Files:**
- Create: `ingress-api/app/__init__.py`, `ingress-api/tests/__init__.py`, `ingress-api/requirements.txt`, `ingress-api/pytest.ini`
- Create: `sync-agent/agent/__init__.py`, `sync-agent/tests/__init__.py`, `sync-agent/requirements.txt`, `sync-agent/pytest.ini`

- [ ] **Step 1: Create empty package files**

```bash
mkdir -p ingress-api/app ingress-api/tests sync-agent/agent sync-agent/tests
touch ingress-api/app/__init__.py ingress-api/tests/__init__.py
touch sync-agent/agent/__init__.py sync-agent/tests/__init__.py
```

- [ ] **Step 2: Write `ingress-api/requirements.txt`**

```
fastapi==0.115.6
uvicorn[standard]==0.34.0
httpx==0.28.1
PyYAML==6.0.2
pytest==8.3.4
```

- [ ] **Step 3: Write `sync-agent/requirements.txt`**

```
httpx==0.28.1
pytest==8.3.4
```

- [ ] **Step 4: Write `ingress-api/pytest.ini` and `sync-agent/pytest.ini`** (identical)

```ini
[pytest]
testpaths = tests
python_files = test_*.py
addopts = -q
```

- [ ] **Step 5: Install deps locally for development**

Run: `cd ingress-api && python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && cd ..`
Run: `cd sync-agent && python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && cd ..`
Expected: installs succeed.

- [ ] **Step 6: Commit**

```bash
git add ingress-api sync-agent && git commit -m "chore: scaffold ingress-api and sync-agent packages"
```

---

## Task 1: Ingress API — config loader

**Files:**
- Create: `ingress-api/app/config.py`
- Test: `ingress-api/tests/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
# ingress-api/tests/test_config.py
import pytest
from app.config import load_settings

def test_load_settings_reads_env(monkeypatch):
    monkeypatch.setenv("INFLUXDB_URL", "http://influx:8086")
    monkeypatch.setenv("INFLUXDB_TOKEN", "tok")
    monkeypatch.setenv("INFLUXDB_ORG", "org")
    monkeypatch.setenv("SOURCES_FILE", "/cfg/sources.yaml")
    s = load_settings()
    assert s.influx_url == "http://influx:8086"
    assert s.influx_token == "tok"
    assert s.influx_org == "org"
    assert s.sources_file == "/cfg/sources.yaml"

def test_load_settings_defaults_sources_file(monkeypatch):
    monkeypatch.setenv("INFLUXDB_URL", "http://influx:8086")
    monkeypatch.setenv("INFLUXDB_TOKEN", "tok")
    monkeypatch.setenv("INFLUXDB_ORG", "org")
    monkeypatch.delenv("SOURCES_FILE", raising=False)
    assert load_settings().sources_file == "/config/sources.yaml"

def test_load_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("INFLUXDB_URL", raising=False)
    monkeypatch.setenv("INFLUXDB_TOKEN", "tok")
    monkeypatch.setenv("INFLUXDB_ORG", "org")
    with pytest.raises(RuntimeError, match="INFLUXDB_URL"):
        load_settings()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ingress-api && . .venv/bin/activate && pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.config'`.

- [ ] **Step 3: Write minimal implementation**

```python
# ingress-api/app/config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    influx_url: str
    influx_token: str
    influx_org: str
    sources_file: str

def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def load_settings() -> Settings:
    return Settings(
        influx_url=_require("INFLUXDB_URL"),
        influx_token=_require("INFLUXDB_TOKEN"),
        influx_org=_require("INFLUXDB_ORG"),
        sources_file=os.environ.get("SOURCES_FILE", "/config/sources.yaml"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add ingress-api/app/config.py ingress-api/tests/test_config.py
git commit -m "feat(api): env settings loader"
```

---

## Task 2: Ingress API — source registry & auth

**Files:**
- Create: `ingress-api/app/registry.py`
- Test: `ingress-api/tests/test_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# ingress-api/tests/test_registry.py
import hashlib, textwrap, pytest
from app.registry import load_registry, SourceRegistry, Source

def _sha(k): return hashlib.sha256(k.encode()).hexdigest()

@pytest.fixture
def reg(tmp_path):
    p = tmp_path / "sources.yaml"
    p.write_text(textwrap.dedent(f"""
        sources:
          site-a:
            bucket: bucket_a
            tags: {{source: site_a}}
            key_sha256: "{_sha('key-a')}"
          site-b:
            bucket: bucket_b
            tags: {{source: site_b}}
            key_sha256: "{_sha('key-b')}"
    """))
    return load_registry(str(p))

def test_authenticate_returns_matching_source(reg):
    s = reg.authenticate("key-a")
    assert isinstance(s, Source)
    assert s.id == "site-a"
    assert s.bucket == "bucket_a"
    assert s.tags == {"source": "site_a"}

def test_authenticate_wrong_key_returns_none(reg):
    assert reg.authenticate("nope") is None

def test_authenticate_empty_key_returns_none(reg):
    assert reg.authenticate("") is None
    assert reg.authenticate(None) is None

def test_load_registry_rejects_duplicate_key(tmp_path):
    p = tmp_path / "s.yaml"
    same = _sha("dup")
    p.write_text(textwrap.dedent(f"""
        sources:
          a: {{bucket: b1, tags: {{}}, key_sha256: "{same}"}}
          b: {{bucket: b2, tags: {{}}, key_sha256: "{same}"}}
    """))
    with pytest.raises(ValueError, match="duplicate"):
        load_registry(str(p))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.registry'`.

- [ ] **Step 3: Write minimal implementation**

```python
# ingress-api/app/registry.py
import hashlib, hmac
from dataclasses import dataclass
import yaml

@dataclass(frozen=True)
class Source:
    id: str
    bucket: str
    tags: dict
    key_sha256: str

class SourceRegistry:
    def __init__(self, sources: list[Source]):
        self._sources = sources
        seen = set()
        for s in sources:
            if s.key_sha256 in seen:
                raise ValueError(f"duplicate key_sha256 for source {s.id}")
            seen.add(s.key_sha256)

    def authenticate(self, api_key: str | None) -> Source | None:
        if not api_key:
            return None
        digest = hashlib.sha256(api_key.encode()).hexdigest()
        for s in self._sources:
            if hmac.compare_digest(digest, s.key_sha256):
                return s
        return None

def load_registry(path: str) -> SourceRegistry:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    sources = [
        Source(id=sid, bucket=cfg["bucket"],
               tags=cfg.get("tags") or {}, key_sha256=cfg["key_sha256"])
        for sid, cfg in (data.get("sources") or {}).items()
    ]
    return SourceRegistry(sources)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_registry.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add ingress-api/app/registry.py ingress-api/tests/test_registry.py
git commit -m "feat(api): source registry with constant-time key auth"
```

---

## Task 3: Ingress API — default-tag injection into line protocol

**Files:**
- Create: `ingress-api/app/lp.py`
- Test: `ingress-api/tests/test_lp.py`

Default tags identify the source downstream. We insert them into each line's tag set, respecting line-protocol escaping (`\,`, `\ `, `\=`). Measurement and tag-set are separated from fields by the first **unescaped space**; measurement ends at the first **unescaped comma**.

- [ ] **Step 1: Write the failing test**

```python
# ingress-api/tests/test_lp.py
from app.lp import inject_tags_into_lp

def test_inject_into_line_with_existing_tags():
    out = inject_tags_into_lp("cpu,host=a load=1 100", {"source": "site_a"})
    assert out == "cpu,host=a,source=site_a load=1 100"

def test_inject_into_line_with_no_tags():
    out = inject_tags_into_lp("cpu load=1 100", {"source": "site_a"})
    assert out == "cpu,source=site_a load=1 100"

def test_inject_escapes_tag_key_and_value():
    out = inject_tags_into_lp("m f=1 1", {"a b": "c,d"})
    assert out == r"m,a\ b=c\,d f=1 1"

def test_inject_respects_escaped_space_in_measurement():
    # measurement "we ll" has an escaped space; first UNescaped space precedes fields
    out = inject_tags_into_lp(r"we\ ll f=1 1", {"s": "x"})
    assert out == r"we\ ll,s=x f=1 1"

def test_inject_multiple_lines_and_skips_blank():
    text = "a f=1 1\n\nb f=2 2\n"
    out = inject_tags_into_lp(text, {"s": "x"})
    assert out == "a,s=x f=1 1\nb,s=x f=2 2"

def test_inject_no_tags_is_passthrough():
    assert inject_tags_into_lp("a f=1 1", {}) == "a f=1 1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_lp.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.lp'`.

- [ ] **Step 3: Write minimal implementation**

```python
# ingress-api/app/lp.py
def _escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")

def _split_unescaped(line: str, sep: str) -> int:
    """Index of the first unescaped `sep` char, or -1."""
    i = 0
    while i < len(line):
        c = line[i]
        if c == "\\":
            i += 2
            continue
        if c == sep:
            return i
        i += 1
    return -1

def _inject_one(line: str, rendered: str) -> str:
    space = _split_unescaped(line, " ")
    head = line if space == -1 else line[:space]
    rest = "" if space == -1 else line[space:]
    return head + rendered + rest

def inject_tags_into_lp(lp: str, tags: dict) -> str:
    if not tags:
        return "\n".join(l for l in lp.splitlines() if l.strip())
    rendered = "".join(f",{_escape(k)}={_escape(v)}" for k, v in tags.items())
    out = [_inject_one(l, rendered) for l in lp.splitlines() if l.strip()]
    return "\n".join(out)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_lp.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add ingress-api/app/lp.py ingress-api/tests/test_lp.py
git commit -m "feat(api): line-protocol default-tag injection with escaping"
```

---

## Task 4: Ingress API — InfluxDB client

**Files:**
- Create: `ingress-api/app/influx.py`
- Test: `ingress-api/tests/test_influx.py`

Wraps the raw InfluxDB v2 HTTP API via an injected `httpx.Client` (so tests use `httpx.MockTransport`, no network).

- [ ] **Step 1: Write the failing test**

```python
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
    with pytest.raises(Exception):
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_influx.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.influx'`.

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_influx.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add ingress-api/app/influx.py ingress-api/tests/test_influx.py
git commit -m "feat(api): InfluxDB client (write/highwater/health)"
```

---

## Task 5: Ingress API — FastAPI app & routes

**Files:**
- Create: `ingress-api/app/main.py`
- Test: `ingress-api/tests/test_main.py`, `ingress-api/tests/conftest.py`

- [ ] **Step 1: Write conftest with a fake InfluxClient and app factory override**

```python
# ingress-api/tests/conftest.py
import hashlib, textwrap, pytest
from fastapi.testclient import TestClient
from app.main import create_app
from app import main as main_mod

def _sha(k): return hashlib.sha256(k.encode()).hexdigest()

class FakeInflux:
    def __init__(self):
        self.writes = []
        self.hw = None
        self.healthy = True
        self.fail_write = False
    def write(self, bucket, lp, gzipped=False):
        if self.fail_write:
            raise RuntimeError("influx down")
        self.writes.append((bucket, lp, gzipped))
    def highwater(self, bucket):
        return self.hw
    def health(self):
        return self.healthy

@pytest.fixture
def sources_file(tmp_path):
    p = tmp_path / "sources.yaml"
    p.write_text(textwrap.dedent(f"""
        sources:
          site-a:
            bucket: bucket_a
            tags: {{source: site_a}}
            key_sha256: "{_sha('key-a')}"
    """))
    return str(p)

@pytest.fixture
def fake_influx():
    return FakeInflux()

@pytest.fixture
def client(sources_file, fake_influx, monkeypatch):
    monkeypatch.setenv("INFLUXDB_URL", "http://x:8086")
    monkeypatch.setenv("INFLUXDB_TOKEN", "t")
    monkeypatch.setenv("INFLUXDB_ORG", "o")
    monkeypatch.setenv("SOURCES_FILE", sources_file)
    app = create_app(influx=fake_influx)
    return TestClient(app)
```

- [ ] **Step 2: Write the failing test**

```python
# ingress-api/tests/test_main.py
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_main.py -v`
Expected: FAIL — cannot import `create_app`.

- [ ] **Step 4: Write minimal implementation**

```python
# ingress-api/app/main.py
import gzip
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from app.config import load_settings
from app.registry import load_registry, Source
from app.influx import InfluxClient
from app.lp import inject_tags_into_lp

def create_app(influx: InfluxClient | None = None) -> FastAPI:
    settings = load_settings()
    registry = load_registry(settings.sources_file)
    influx = influx or InfluxClient(settings.influx_url, settings.influx_token, settings.influx_org)
    app = FastAPI(title="boat-telemetry-ingress")

    def auth(authorization: str | None = Header(default=None)) -> Source:
        key = None
        if authorization and authorization.lower().startswith("bearer "):
            key = authorization[7:]
        src = registry.authenticate(key)
        if src is None:
            raise HTTPException(status_code=401, detail="invalid api key")
        return src

    @app.post("/v1/ingest")
    async def ingest(request: Request, src: Source = Depends(auth)):
        body = await request.body()
        if request.headers.get("content-encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
        text = body.decode("utf-8", errors="strict").strip()
        if not text:
            raise HTTPException(status_code=400, detail="empty body")
        lp = inject_tags_into_lp(text, src.tags).encode("utf-8")
        try:
            influx.write(src.bucket, lp)
        except Exception:
            raise HTTPException(status_code=502, detail="influxdb write failed")
        return {"written": True}

    @app.get("/v1/highwater")
    def highwater(src: Source = Depends(auth)):
        try:
            hw = influx.highwater(src.bucket)
        except Exception:
            raise HTTPException(status_code=502, detail="influxdb query failed")
        return {"highwater": hw.isoformat() if hw else None}

    @app.get("/health")
    def health():
        return {"status": "ok", "influxdb": influx.health()}

    return app
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_main.py -v`
Expected: PASS (7 passed).

- [ ] **Step 6: Run the full API suite**

Run: `pytest -v`
Expected: PASS (all tasks 1–5 green).

- [ ] **Step 7: Commit**

```bash
git add ingress-api/app/main.py ingress-api/tests/test_main.py ingress-api/tests/conftest.py
git commit -m "feat(api): FastAPI routes for ingest/highwater/health with bearer auth"
```

---

## Task 6: Ingress API — container & config templates

**Files:**
- Create: `ingress-api/Dockerfile`, `ingress-api/docker-compose.yml`, `ingress-api/sources.example.yaml`, `ingress-api/.env.example`, `ingress-api/README.md`

- [ ] **Step 1: Write `ingress-api/Dockerfile`**

```dockerfile
FROM python:3.12-alpine
RUN adduser -D appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
USER appuser
EXPOSE 8080
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080"]
```

- [ ] **Step 2: Write `ingress-api/docker-compose.yml`**

```yaml
services:
  ingress-api:
    build: .
    container_name: boat-ingress-api
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - INFLUXDB_URL=${INFLUXDB_URL}
      - INFLUXDB_TOKEN=${INFLUXDB_TOKEN}
      - INFLUXDB_ORG=${INFLUXDB_ORG:-smart_home}
      - SOURCES_FILE=/config/sources.yaml
    volumes:
      - ./sources.yaml:/config/sources.yaml:ro
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

- [ ] **Step 3: Write `ingress-api/sources.example.yaml`**

```yaml
# Copy to sources.yaml (gitignored) and fill in real values.
# key_sha256 = sha256 hex of the API key you give that source:
#   printf '%s' 'THE-API-KEY' | sha256sum
sources:
  site-a:
    bucket: "<bucket-for-site-a>"
    tags: { source: "<tag-value>" }
    key_sha256: "<sha256-hex-of-api-key>"
```

- [ ] **Step 4: Write `ingress-api/.env.example`**

```
INFLUXDB_URL=http://influxdb:8086
INFLUXDB_TOKEN=
INFLUXDB_ORG=smart_home
```

- [ ] **Step 5: Write `ingress-api/README.md`**

````markdown
# ingress-api

Central HTTP API that authenticates each source by API key, applies the
source's default tags, and writes line protocol to that source's InfluxDB
bucket. Holds the only InfluxDB credentials.

## Configure
1. `cp .env.example .env` and set `INFLUXDB_TOKEN`.
2. `cp sources.example.yaml sources.yaml` and add each source. Generate a key
   hash with: `printf '%s' 'THE-API-KEY' | sha256sum`.

## Run
```bash
docker compose up -d --build
```

## Endpoints
- `GET /v1/highwater` (Bearer key) → `{"highwater": "<iso8601>"|null}`
- `POST /v1/ingest` (Bearer key, body=line protocol, optional `Content-Encoding: gzip`) → `{"written": true}`
- `GET /health` → `{"status":"ok","influxdb":true|false}`

## Test
```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && pytest
```
````

- [ ] **Step 6: Verify the image builds and starts**

Run: `cd ingress-api && docker build -t ingress-api-test . && cd ..`
Expected: build succeeds.

- [ ] **Step 7: Commit**

```bash
git add ingress-api/Dockerfile ingress-api/docker-compose.yml ingress-api/sources.example.yaml ingress-api/.env.example ingress-api/README.md
git commit -m "feat(api): Dockerfile, compose, and config templates"
```

---

## Task 7: Sync agent — config loader

**Files:**
- Create: `sync-agent/agent/config.py`
- Test: `sync-agent/tests/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
# sync-agent/tests/test_config.py
import pytest
from agent.config import load_settings

BASE = {
    "API_URL": "http://api:8080", "API_KEY": "k",
    "LOCAL_INFLUX_URL": "http://localhost:8086", "LOCAL_INFLUX_TOKEN": "t",
    "LOCAL_INFLUX_ORG": "o", "LOCAL_INFLUX_BUCKET": "b",
}

def _set(monkeypatch, **over):
    for k, v in {**BASE, **over}.items():
        monkeypatch.setenv(k, v)

def test_load_defaults(monkeypatch):
    _set(monkeypatch)
    s = load_settings()
    assert s.api_url == "http://api:8080"
    assert s.poll_interval == 60
    assert s.window_seconds == 21600
    assert s.overlap_seconds == 600
    assert s.backfill_floor == "-30d"

def test_load_overrides_numeric(monkeypatch):
    _set(monkeypatch, POLL_INTERVAL="30", WINDOW_SECONDS="3600", OVERLAP_SECONDS="120")
    s = load_settings()
    assert (s.poll_interval, s.window_seconds, s.overlap_seconds) == (30, 3600, 120)

def test_missing_required_raises(monkeypatch):
    _set(monkeypatch)
    monkeypatch.delenv("API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="API_KEY"):
        load_settings()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd sync-agent && . .venv/bin/activate && pytest tests/test_config.py -v`
Expected: FAIL — no module `agent.config`.

- [ ] **Step 3: Write minimal implementation**

```python
# sync-agent/agent/config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    api_url: str
    api_key: str
    local_influx_url: str
    local_influx_token: str
    local_influx_org: str
    local_influx_bucket: str
    poll_interval: int
    window_seconds: int
    overlap_seconds: int
    backfill_floor: str

def _require(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

def load_settings() -> Settings:
    return Settings(
        api_url=_require("API_URL").rstrip("/"),
        api_key=_require("API_KEY"),
        local_influx_url=_require("LOCAL_INFLUX_URL").rstrip("/"),
        local_influx_token=_require("LOCAL_INFLUX_TOKEN"),
        local_influx_org=_require("LOCAL_INFLUX_ORG"),
        local_influx_bucket=_require("LOCAL_INFLUX_BUCKET"),
        poll_interval=int(os.environ.get("POLL_INTERVAL", "60")),
        window_seconds=int(os.environ.get("WINDOW_SECONDS", "21600")),
        overlap_seconds=int(os.environ.get("OVERLAP_SECONDS", "600")),
        backfill_floor=os.environ.get("BACKFILL_FLOOR", "-30d"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add sync-agent/agent/config.py sync-agent/tests/test_config.py
git commit -m "feat(agent): env settings loader"
```

---

## Task 8: Sync agent — annotated CSV → line protocol

**Files:**
- Create: `sync-agent/agent/csvlp.py`
- Test: `sync-agent/tests/test_csvlp.py`

Converts InfluxDB annotated-CSV query output to line protocol, one line per record (`measurement,tags field=value ts`). Uses the `#datatype` annotation row to format `_value` (double→bare, long→`i` suffix, string→quoted, boolean→`true/false`) and emits nanosecond timestamps.

- [ ] **Step 1: Write the failing test**

```python
# sync-agent/tests/test_csvlp.py
from agent.csvlp import annotated_csv_to_lp

def test_double_field_with_tag():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement,host\r\n"
        ",_result,0,2026-01-01T00:00:00Z,2026-01-01T01:00:00Z,2026-06-14T18:40:00Z,1.5,load,cpu,a\r\n"
    )
    assert annotated_csv_to_lp(csv) == "cpu,host=a load=1.5 1781462400000000000"

def test_long_field_gets_i_suffix_and_escapes_tag():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement,room name\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,42,count,sensor,back cabin\r\n"
    )
    assert annotated_csv_to_lp(csv) == r"sensor,room\ name=back\ cabin count=42i 1781462400000000000"

def test_string_field_is_quoted():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ',_result,0,,,2026-06-14T18:40:00Z,under way,state,navigation\r\n'
    )
    assert annotated_csv_to_lp(csv) == 'navigation state="under way" 1781462400000000000'

def test_boolean_field():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,boolean,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,true,enabled,watchdog\r\n"
    )
    assert annotated_csv_to_lp(csv) == "watchdog enabled=true 1781462400000000000"

def test_empty_result_yields_empty_string():
    assert annotated_csv_to_lp("\r\n") == ""
    assert annotated_csv_to_lp("") == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_csvlp.py -v`
Expected: FAIL — no module `agent.csvlp`.

- [ ] **Step 3: Write minimal implementation**

```python
# sync-agent/agent/csvlp.py
from datetime import datetime

_RESERVED = {"", "result", "table", "_start", "_stop", "_time", "_value",
             "_field", "_measurement"}

def _esc_tag(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")

def _esc_meas(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace(" ", r"\ ")

def _to_ns(rfc3339: str) -> int:
    dt = datetime.fromisoformat(rfc3339.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)

def _fmt_value(raw: str, datatype: str) -> str:
    if datatype in ("long", "unsignedLong"):
        return f"{int(raw)}i"
    if datatype == "boolean":
        return "true" if raw.lower() == "true" else "false"
    if datatype in ("double", "float"):
        return raw
    # string (or anything else) → quoted
    return '"' + raw.replace("\\", "\\\\").replace('"', '\\"') + '"'

def annotated_csv_to_lp(csv_text: str) -> str:
    datatypes: list[str] = []
    header: list[str] = []
    lines = []
    for raw in csv_text.splitlines():
        row = raw.rstrip("\r")
        if not row.strip():
            continue
        cells = row.split(",")
        if row.startswith("#datatype"):
            datatypes = cells
            continue
        if row.startswith("#"):
            continue
        if "_measurement" in cells:        # header row
            header = cells
            continue
        if not header:
            continue
        rec = dict(zip(header, cells))
        meas = _esc_meas(rec["_measurement"])
        tags = "".join(
            f",{_esc_tag(col)}={_esc_tag(rec[col])}"
            for col in header
            if col not in _RESERVED and rec.get(col)
        )
        vtype = datatypes[header.index("_value")] if datatypes else "double"
        value = _fmt_value(rec["_value"], vtype)
        ns = _to_ns(rec["_time"])
        lines.append(f"{meas}{tags} {rec['_field']}={value} {ns}")
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_csvlp.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add sync-agent/agent/csvlp.py sync-agent/tests/test_csvlp.py
git commit -m "feat(agent): annotated-CSV to line-protocol serializer"
```

---

## Task 9: Sync agent — local InfluxDB window query

**Files:**
- Create: `sync-agent/agent/local_influx.py`
- Test: `sync-agent/tests/test_local_influx.py`

- [ ] **Step 1: Write the failing test**

```python
# sync-agent/tests/test_local_influx.py
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
        seen["url"] = str(req.url); seen["body"] = req.content.decode(); seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, text=CSV)
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://localhost:8086")
    li = LocalInflux("http://localhost:8086", "tok", "org", "mybucket", http=http)
    start = datetime(2026, 6, 14, 18, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 6, 14, 19, 0, tzinfo=timezone.utc)
    lp = li.query_window(start, stop)
    assert lp == "cpu,host=a load=1.5 1781462400000000000"
    assert "/api/v2/query" in seen["url"] and "org=org" in seen["url"]
    assert 'from(bucket:"mybucket")' in seen["body"]
    assert "2026-06-14T18:00:00+00:00" in seen["body"]
    assert "2026-06-14T19:00:00+00:00" in seen["body"]
    assert seen["auth"] == "Token tok"

def test_query_window_empty_returns_empty():
    http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, text="\r\n")),
                        base_url="http://localhost:8086")
    li = LocalInflux("http://localhost:8086", "tok", "org", "b", http=http)
    s = datetime(2026, 6, 14, tzinfo=timezone.utc)
    assert li.query_window(s, s) == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_local_influx.py -v`
Expected: FAIL — no module `agent.local_influx`.

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_local_influx.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add sync-agent/agent/local_influx.py sync-agent/tests/test_local_influx.py
git commit -m "feat(agent): local InfluxDB window query to line protocol"
```

---

## Task 10: Sync agent — API client

**Files:**
- Create: `sync-agent/agent/api_client.py`
- Test: `sync-agent/tests/test_api_client.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_api_client.py -v`
Expected: FAIL — no module `agent.api_client`.

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_api_client.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add sync-agent/agent/api_client.py sync-agent/tests/test_api_client.py
git commit -m "feat(agent): API client (highwater/ingest, gzip)"
```

---

## Task 11: Sync agent — sync core (`run_once`)

**Files:**
- Create: `sync-agent/agent/sync.py`
- Test: `sync-agent/tests/test_sync.py`

`run_once` is the heart: get highwater → compute start (with overlap, or backfill floor) → iterate bounded windows up to `now` → query local → if non-empty, POST → advance. On a POST failure it raises (caller retries next cycle); nothing is lost because the next cycle re-reads the watermark.

- [ ] **Step 1: Write the failing test**

```python
# sync-agent/tests/test_sync.py
from datetime import datetime, timedelta, timezone
import pytest
from agent.sync import run_once

UTC = timezone.utc

class FakeApi:
    def __init__(self, hw): self.hw = hw; self.posts = []; self.fail_after = None
    def get_highwater(self): return self.hw
    def post_ingest(self, lp):
        if self.fail_after is not None and len(self.posts) >= self.fail_after:
            raise RuntimeError("api down")
        self.posts.append(lp)

class FakeLocal:
    def __init__(self): self.queries = []
    def query_window(self, start, stop):
        self.queries.append((start, stop))
        return f"m,w=x f=1 {int(start.timestamp())}"   # always non-empty

def test_backfill_paginates_in_windows(monkeypatch):
    now = datetime(2026, 6, 14, 12, 0, tzinfo=UTC)
    hw = now - timedelta(hours=15)              # 15h behind
    api = FakeApi(hw); local = FakeLocal()
    sent = run_once(api, local, window_seconds=21600, overlap_seconds=600,
                    backfill_floor="-30d", now=now)
    # start = hw - 600s; windows of 6h until now → ceil((15h+10m)/6h) = 3 windows
    assert len(local.queries) == 3
    assert sent == 3
    # first window starts at hw - overlap
    assert local.queries[0][0] == hw - timedelta(seconds=600)
    # windows are contiguous and clamp to now
    assert local.queries[-1][1] == now

def test_no_highwater_uses_backfill_floor():
    now = datetime(2026, 6, 14, 12, 0, tzinfo=UTC)
    api = FakeApi(None); local = FakeLocal()
    run_once(api, local, window_seconds=21600, overlap_seconds=600,
             backfill_floor="-2h", now=now)
    assert local.queries[0][0] == now - timedelta(hours=2)

def test_failure_stops_and_propagates_without_full_advance():
    now = datetime(2026, 6, 14, 12, 0, tzinfo=UTC)
    hw = now - timedelta(hours=15)
    api = FakeApi(hw); api.fail_after = 1       # 2nd post fails
    local = FakeLocal()
    with pytest.raises(RuntimeError):
        run_once(api, local, window_seconds=21600, overlap_seconds=600,
                 backfill_floor="-30d", now=now)
    assert len(api.posts) == 1                  # only the first window committed

def test_empty_window_is_not_posted():
    now = datetime(2026, 6, 14, 12, 0, tzinfo=UTC)
    hw = now - timedelta(minutes=5)
    api = FakeApi(hw)
    class Empty(FakeLocal):
        def query_window(self, start, stop):
            self.queries.append((start, stop)); return ""
    local = Empty()
    sent = run_once(api, local, window_seconds=21600, overlap_seconds=600,
                    backfill_floor="-30d", now=now)
    assert sent == 0 and api.posts == []
    assert len(local.queries) == 1              # still queried once
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_sync.py -v`
Expected: FAIL — no module `agent.sync`.

- [ ] **Step 3: Write minimal implementation**

```python
# sync-agent/agent/sync.py
import re
from datetime import datetime, timedelta, timezone

_FLOOR_RE = re.compile(r"^-(\d+)([smhdw])$")
_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

def _floor_to_dt(floor: str, now: datetime) -> datetime:
    m = _FLOOR_RE.match(floor)
    if not m:
        raise ValueError(f"invalid BACKFILL_FLOOR: {floor!r} (expected like -30d)")
    return now - timedelta(seconds=int(m.group(1)) * _UNIT[m.group(2)])

def run_once(api, local, *, window_seconds: int, overlap_seconds: int,
             backfill_floor: str, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    hw = api.get_highwater()
    if hw is None:
        start = _floor_to_dt(backfill_floor, now)
    else:
        start = hw - timedelta(seconds=overlap_seconds)
    sent = 0
    window = timedelta(seconds=window_seconds)
    while start < now:
        stop = min(start + window, now)
        lp = local.query_window(start, stop)
        if lp.strip():
            api.post_ingest(lp.encode("utf-8"))   # raises on failure → caller retries
            sent += 1
        start = stop
    return sent
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_sync.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Run full agent suite**

Run: `pytest -v`
Expected: PASS (tasks 7–11 green).

- [ ] **Step 6: Commit**

```bash
git add sync-agent/agent/sync.py sync-agent/tests/test_sync.py
git commit -m "feat(agent): sync core with windowed backfill and no-loss semantics"
```

---

## Task 12: Sync agent — main loop & container

**Files:**
- Create: `sync-agent/agent/main.py`, `sync-agent/Dockerfile`, `sync-agent/docker-compose.yml`, `sync-agent/.env.example`, `sync-agent/README.md`

- [ ] **Step 1: Write `sync-agent/agent/main.py`**

```python
# sync-agent/agent/main.py
import logging, time
from agent.config import load_settings
from agent.api_client import ApiClient
from agent.local_influx import LocalInflux
from agent.sync import run_once

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync-agent")

def main():
    s = load_settings()
    api = ApiClient(s.api_url, s.api_key)
    local = LocalInflux(s.local_influx_url, s.local_influx_token, s.local_influx_org, s.local_influx_bucket)
    log.info("sync-agent started: api=%s bucket=%s interval=%ss",
             s.api_url, s.local_influx_bucket, s.poll_interval)
    while True:
        try:
            sent = run_once(api, local, window_seconds=s.window_seconds,
                            overlap_seconds=s.overlap_seconds, backfill_floor=s.backfill_floor)
            if sent:
                log.info("cycle complete: %d window(s) forwarded", sent)
        except Exception as e:
            log.warning("cycle failed (will retry): %s", e)
        time.sleep(s.poll_interval)

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write `sync-agent/Dockerfile`**

```dockerfile
FROM python:3.12-alpine
RUN adduser -D appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent ./agent
USER appuser
CMD ["python3", "-u", "-m", "agent.main"]
```

- [ ] **Step 3: Write `sync-agent/docker-compose.yml`**

```yaml
services:
  sync-agent:
    build: .
    container_name: boat-sync-agent
    restart: unless-stopped
    environment:
      - API_URL=${API_URL}
      - API_KEY=${API_KEY}
      - LOCAL_INFLUX_URL=${LOCAL_INFLUX_URL:-http://localhost:8086}
      - LOCAL_INFLUX_TOKEN=${LOCAL_INFLUX_TOKEN}
      - LOCAL_INFLUX_ORG=${LOCAL_INFLUX_ORG}
      - LOCAL_INFLUX_BUCKET=${LOCAL_INFLUX_BUCKET}
      - POLL_INTERVAL=${POLL_INTERVAL:-60}
      - WINDOW_SECONDS=${WINDOW_SECONDS:-21600}
      - OVERLAP_SECONDS=${OVERLAP_SECONDS:-600}
      - BACKFILL_FLOOR=${BACKFILL_FLOOR:--30d}
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

- [ ] **Step 4: Write `sync-agent/.env.example`**

```
API_URL=http://boat-ingress-api:8080
API_KEY=
LOCAL_INFLUX_URL=http://localhost:8086
LOCAL_INFLUX_TOKEN=
LOCAL_INFLUX_ORG=
LOCAL_INFLUX_BUCKET=
POLL_INTERVAL=60
WINDOW_SECONDS=21600
OVERLAP_SECONDS=600
BACKFILL_FLOOR=-30d
```

- [ ] **Step 5: Write `sync-agent/README.md`**

````markdown
# sync-agent

Stateless per-site agent. Each cycle it asks the ingress API for its
high-water mark, queries the local InfluxDB forward in bounded windows, and
POSTs gzipped line protocol. The local InfluxDB is the durable buffer, so the
agent holds no state and is safe to restart/reimage.

## Configure & run
```bash
cp .env.example .env   # set API_KEY and LOCAL_INFLUX_* (and API_URL = tailnet name of the API)
docker compose up -d --build
```

## Tunables
- `WINDOW_SECONDS` (default 21600 = 6h): max time span per request — bound payload size on cellular.
- `OVERLAP_SECONDS` (default 600): re-send window past the watermark (idempotent) to cover per-measurement write lag.
- `BACKFILL_FLOOR` (default `-30d`): how far back to start when the API reports no data yet.
- `POLL_INTERVAL` (default 60s).

## Test
```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && pytest
```
````

- [ ] **Step 6: Verify image builds**

Run: `cd sync-agent && docker build -t sync-agent-test . && cd ..`
Expected: build succeeds.

- [ ] **Step 7: Commit**

```bash
git add sync-agent/agent/main.py sync-agent/Dockerfile sync-agent/docker-compose.yml sync-agent/.env.example sync-agent/README.md
git commit -m "feat(agent): main loop and container"
```

---

## Task 13: End-to-end test (real InfluxDB, both containers)

**Files:**
- Create: `docker-compose.e2e.yml` (repo root), `tests/e2e/test_e2e.py`, `tests/e2e/requirements.txt`, `tests/e2e/seed.flux`

Proves the whole path: seed a source InfluxDB, run the API against a destination InfluxDB, run the agent, and assert data lands in the destination bucket with the injected source tag. Then simulate an outage gap and assert backfill catches up.

- [ ] **Step 1: Write `docker-compose.e2e.yml`**

```yaml
# Two InfluxDBs (source = the "boat", dest = "home") + the API + the agent.
services:
  influx-src:
    image: influxdb:2.7
    environment:
      DOCKER_INFLUXDB_INIT_MODE: setup
      DOCKER_INFLUXDB_INIT_USERNAME: ci
      DOCKER_INFLUXDB_INIT_PASSWORD: ci-password
      DOCKER_INFLUXDB_INIT_ORG: boat
      DOCKER_INFLUXDB_INIT_BUCKET: signalk
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: src-token
    ports: ["8186:8086"]
  influx-dst:
    image: influxdb:2.7
    environment:
      DOCKER_INFLUXDB_INIT_MODE: setup
      DOCKER_INFLUXDB_INIT_USERNAME: ci
      DOCKER_INFLUXDB_INIT_PASSWORD: ci-password
      DOCKER_INFLUXDB_INIT_ORG: smart_home
      DOCKER_INFLUXDB_INIT_BUCKET: site_a
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: dst-token
    ports: ["8286:8086"]
  api:
    build: ./ingress-api
    depends_on: [influx-dst]
    environment:
      INFLUXDB_URL: http://influx-dst:8086
      INFLUXDB_TOKEN: dst-token
      INFLUXDB_ORG: smart_home
      SOURCES_FILE: /config/sources.yaml
    volumes:
      - ./tests/e2e/sources.e2e.yaml:/config/sources.yaml:ro
    ports: ["8080:8080"]
  agent:
    build: ./sync-agent
    depends_on: [api, influx-src]
    environment:
      API_URL: http://api:8080
      API_KEY: e2e-key
      LOCAL_INFLUX_URL: http://influx-src:8086
      LOCAL_INFLUX_TOKEN: src-token
      LOCAL_INFLUX_ORG: boat
      LOCAL_INFLUX_BUCKET: signalk
      POLL_INTERVAL: "5"
      BACKFILL_FLOOR: "-1h"
```

- [ ] **Step 2: Write `tests/e2e/sources.e2e.yaml`**

```yaml
# key_sha256 = sha256('e2e-key')
sources:
  site-a:
    bucket: site_a
    tags: { source: site_a }
    key_sha256: "a781b3e13d2b80957d1b925804148f666cc87adb35c1481686a9687f27f8d8a0"
```

Note: regenerate if the key changes — `printf '%s' 'e2e-key' | sha256sum`.

- [ ] **Step 3: Write `tests/e2e/requirements.txt`**

```
httpx==0.28.1
pytest==8.3.4
```

- [ ] **Step 4: Write `tests/e2e/test_e2e.py`**

```python
# tests/e2e/test_e2e.py
import subprocess, time, httpx, pytest

SRC = "http://localhost:8186"
DST = "http://localhost:8286"

def _write_src(lp: str):
    r = httpx.post(f"{SRC}/api/v2/write", params={"org": "boat", "bucket": "signalk", "precision": "s"},
                   headers={"Authorization": "Token src-token"}, content=lp)
    r.raise_for_status()

def _count_dst() -> int:
    flux = ('from(bucket:"site_a") |> range(start: 0) '
            '|> filter(fn:(r)=> r.source=="site_a") |> count() '
            '|> keep(columns:["_value"])')
    r = httpx.post(f"{DST}/api/v2/query", params={"org": "smart_home"},
                   headers={"Authorization": "Token dst-token", "Content-Type": "application/vnd.flux",
                            "Accept": "application/csv"}, content=flux)
    r.raise_for_status()
    total = 0
    for line in r.text.splitlines():
        c = line.strip().split(",")
        if c and c[-1].isdigit():
            total += int(c[-1])
    return total

def _wait(pred, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred(): return True
        except Exception:
            pass
        time.sleep(2)
    return False

@pytest.fixture(scope="module")
def stack():
    subprocess.run(["docker", "compose", "-f", "docker-compose.e2e.yml", "up", "-d", "--build"], check=True)
    try:
        assert _wait(lambda: httpx.get(f"{SRC}/health").status_code == 200)
        assert _wait(lambda: httpx.get(f"{DST}/health").status_code == 200)
        yield
    finally:
        subprocess.run(["docker", "compose", "-f", "docker-compose.e2e.yml", "down", "-v"], check=True)

def test_live_and_backfill(stack):
    # Seed two points within the backfill floor window
    now = int(time.time())
    _write_src(f"cpu,host=a load=1.0 {now-120}\ncpu,host=a load=2.0 {now-60}")
    assert _wait(lambda: _count_dst() >= 2, timeout=60), "live points did not arrive in destination"
    # Simulate a later burst (the agent should pick it up from the watermark forward)
    _write_src(f"cpu,host=a load=3.0 {now+1}")
    assert _wait(lambda: _count_dst() >= 3, timeout=60), "backfill/continuation did not arrive"
```

- [ ] **Step 5: Run the end-to-end test (requires Docker)**

Run: `python3 -m venv .venv-e2e && . .venv-e2e/bin/activate && pip install -r tests/e2e/requirements.txt && pytest tests/e2e/test_e2e.py -v`
Expected: PASS (1 passed). The stack is torn down automatically.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.e2e.yml tests/e2e
git commit -m "test: end-to-end relay + backfill across two InfluxDBs"
```

---

## Task 14: Cutover runbook

**Files:**
- Create: `docs/runbooks/cutover.md`

- [ ] **Step 1: Write `docs/runbooks/cutover.md`**

````markdown
# Cutover Runbook

Goal: move each boat from the legacy export/rsync/cron pipeline to the
API + agent, with no data loss and dashboards staying green.

## 0. Prereqs
- Ingress API deployed (Komodo stack) on a host reachable on the tailnet, with
  `INFLUXDB_*` set to the central InfluxDB and `sources.yaml` listing each source
  against its **existing** bucket (so current dashboards keep working).
- Each source has an API key; its `sha256` is in `sources.yaml`.

## 1. Per boat — deploy the agent (parallel run)
1. On the boat, create the `sync-agent` stack `.env`: `API_URL` = the API's
   tailnet name, `API_KEY` = this boat's key, `LOCAL_INFLUX_*` = the boat's
   local InfluxDB and its existing bucket. Set `BACKFILL_FLOOR` to cover the
   current gap (e.g. `-30d`) for the first run.
2. `docker compose up -d --build`.
3. Watch logs: `docker logs -f boat-sync-agent` — expect "window(s) forwarded".

## 2. Verify parity
- Central InfluxDB point count for the boat's bucket should climb to match local.
- Spot-check timestamps/measurements in Grafana against the boat's local InfluxDB.
- Confirm `GET /v1/highwater` (with the boat's key) advances toward "now".

## 3. Retire the legacy pipeline (only after parity holds)
- On the legacy import host, disable the boat's cron line(s) for the old import script.
- Stop the boat's old export cron (`export_and_sync.sh`).
- Keep the old scripts in place (disabled) as a fallback for one season.

## 4. Rollback
- Re-enable the legacy cron lines; stop the `sync-agent` stack. Because writes
  are idempotent, running both briefly is safe.
````

- [ ] **Step 2: Commit**

```bash
git add docs/runbooks/cutover.md
git commit -m "docs: cutover runbook"
```

- [ ] **Step 3: Push everything**

```bash
git push origin main
```

---

## Deferred (explicitly NOT in this plan)

- **Per-source "last ingest age" metric / dashboard.** The spec lists this under
  observability as "exposable to a dashboard later." It is intentionally out of
  scope for v1 — the API already logs writes, and there is no stale-alerting (by
  design, since sites are seasonal). Add later as a small in-memory
  last-write-timestamp map exposed on `/health` or a `/metrics` endpoint if a
  dashboard panel is wanted.
- **Per-measurement watermark.** v1 uses a single bucket-level max `_time` with
  an `OVERLAP` re-send (see spec "Watermark assumption"). Only revisit if a
  lagging series is actually observed.

## Done criteria

- `cd ingress-api && pytest` → all green; `cd sync-agent && pytest` → all green.
- `pytest tests/e2e/test_e2e.py` → green (Docker available).
- Both images build. API serves `/health`; agent forwards windows and backfills.
- Cutover runbook present. No bucket names, tokens, IPs, or hostnames committed.
