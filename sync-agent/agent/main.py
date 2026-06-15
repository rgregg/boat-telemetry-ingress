# sync-agent/agent/main.py
import logging, time
from agent.config import load_settings
from agent.api_client import ApiClient
from agent.local_influx import LocalInflux
from agent.sync import run_once

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync-agent")

def main():
    s = load_settings()
    api = ApiClient(s.api_url, s.api_key)
    local = LocalInflux(s.local_influx_url, s.local_influx_token, s.local_influx_org, s.local_influx_bucket)
    log.info("sync-agent started: api=%s bucket=%s interval=%ss",
             s.api_url, s.local_influx_bucket, s.poll_interval)
    while True:
        try:
            sent = run_once(api, local, window_seconds=s.window_seconds,
                            overlap_seconds=s.overlap_seconds, backfill_floor=s.backfill_floor)
            if sent:
                log.info("cycle complete: %d window(s) forwarded", sent)
        except Exception as e:
            log.warning("cycle failed (will retry): %s", e)
        time.sleep(s.poll_interval)

if __name__ == "__main__":
    main()
