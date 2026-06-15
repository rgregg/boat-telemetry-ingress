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
