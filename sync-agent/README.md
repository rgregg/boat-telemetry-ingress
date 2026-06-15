# sync-agent

Stateless per-site agent. Each cycle it asks the ingress API for its
high-water mark, queries the local InfluxDB forward in bounded windows, and
POSTs gzipped line protocol. The local InfluxDB is the durable buffer, so the
agent holds no state and is safe to restart/reimage.

## Configure & run
```bash
cp .env.example .env   # set API_KEY and LOCAL_INFLUX_* (and API_URL = tailnet name of the API)
docker compose up -d --build
```

## Tunables
- `WINDOW_SECONDS` (default 21600 = 6h): max time span per request — bound payload size on cellular.
- `OVERLAP_SECONDS` (default 600): re-send window past the watermark (idempotent) to cover per-measurement write lag.
- `BACKFILL_FLOOR` (default `-30d`): how far back to start when the API reports no data yet.
- `POLL_INTERVAL` (default 60s).

## Failure handling
- **Transient errors** (API unreachable, InfluxDB 5xx, network) — the cycle stops
  and retries next `POLL_INTERVAL`. The watermark only advances on confirmed
  writes, so nothing is lost and gaps backfill automatically.
- **Unwritable data** (the API returns `400` because InfluxDB rejected the
  payload — malformed line protocol or a field-type conflict) — that window is
  **logged at ERROR and skipped** rather than retried forever, so one bad point
  cannot stall all newer telemetry. InfluxDB applies partial writes, so the valid
  points in the window still land; only the genuinely unwritable points are
  dropped. Watch the agent logs for `dropping unwritable window` and fix the
  upstream type conflict if it recurs.

## Test
```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && pytest
```
