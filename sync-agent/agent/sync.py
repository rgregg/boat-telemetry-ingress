# sync-agent/agent/sync.py
import re
import logging
import httpx
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

_FLOOR_RE = re.compile(r"^-(\d+)([smhdw])$")
_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

def _floor_to_dt(floor: str, now: datetime) -> datetime:
    m = _FLOOR_RE.match(floor)
    if not m:
        raise ValueError(f"invalid BACKFILL_FLOOR: {floor!r} (expected like -30d)")
    return now - timedelta(seconds=int(m.group(1)) * _UNIT[m.group(2)])

def run_once(api, local, *, window_seconds: int, overlap_seconds: int,
             backfill_floor: str, now: datetime | None = None) -> int:
    if window_seconds <= 0:
        raise ValueError(f"window_seconds must be positive, got {window_seconds}")
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        raise ValueError("now must be tz-aware")
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
            try:
                api.post_ingest(lp.encode("utf-8"))
                sent += 1
            except httpx.HTTPStatusError as exc:
                # 400 = the API/InfluxDB rejected this window as unwritable (not
                # retryable). Drop it with a loud log so one poison window can't
                # block all newer data; any other status propagates → caller retries.
                if exc.response.status_code == 400:
                    log.error("dropping unwritable window [%s, %s): %s", start, stop, exc)
                else:
                    raise
        start = stop
    return sent
