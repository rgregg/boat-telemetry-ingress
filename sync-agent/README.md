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

## Test
```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && pytest
```
