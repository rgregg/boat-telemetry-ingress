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
