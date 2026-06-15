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
