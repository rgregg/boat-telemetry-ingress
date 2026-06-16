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
    r = httpx.post(f"{DST}/api/v2/query", params={"org": "e2e_org"},
                   headers={"Authorization": "Token dst-token", "Content-Type": "application/vnd.flux",
                            "Accept": "application/csv"}, content=flux)
    r.raise_for_status()
    total = 0
    for line in r.text.splitlines():
        c = line.strip().split(",")
        if c and c[-1].isdigit():
            total += int(c[-1])
    return total

def _dst_string_value():
    """Latest value of the string field `state.value` in the destination, or None."""
    flux = ('from(bucket:"site_a") |> range(start: 0) '
            '|> filter(fn:(r)=> r._measurement=="state" and r._field=="value") '
            '|> last() |> keep(columns:["_value"])')
    r = httpx.post(f"{DST}/api/v2/query", params={"org": "e2e_org"},
                   headers={"Authorization": "Token dst-token", "Content-Type": "application/vnd.flux",
                            "Accept": "application/csv"}, content=flux)
    r.raise_for_status()
    for line in r.text.splitlines():
        c = line.strip().split(",")
        if len(c) >= 2 and c[1] == "_result":
            return c[-1]
    return None

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
    # Seed float points AND a string-field point within the backfill window.
    # The string field is a regression guard: the agent must request the datatype
    # annotation and serialize strings QUOTED. Without that, the string serializes
    # as a bare token, InfluxDB rejects the whole batch (code:invalid), and nothing
    # arrives — so this also makes the float assertions fail loudly.
    now = int(time.time())
    _write_src(f"cpu,host=a load=1.0 {now-120}\n"
               f"cpu,host=a load=2.0 {now-60}\n"
               f'state,host=a value="POWER_ON" {now-90}')
    assert _wait(lambda: _count_dst() >= 2, timeout=60), "live points did not arrive in destination"
    assert _wait(lambda: _dst_string_value() == "POWER_ON", timeout=60), \
        "string field did not round-trip correctly (serialization/type regression)"
    # Simulate a later burst (the agent should pick it up from the watermark forward)
    _write_src(f"cpu,host=a load=3.0 {now+1}")
    assert _wait(lambda: _count_dst() >= 3, timeout=60), "backfill/continuation did not arrive"
