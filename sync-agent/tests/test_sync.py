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
