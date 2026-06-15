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
