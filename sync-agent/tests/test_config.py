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
