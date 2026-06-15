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
