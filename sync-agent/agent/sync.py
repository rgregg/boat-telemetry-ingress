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
