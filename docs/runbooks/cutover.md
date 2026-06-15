# Cutover Runbook

Goal: move each boat from the legacy export/rsync/cron pipeline to the
API + agent, with no data loss and dashboards staying green.

## 0. Prereqs
- Ingress API deployed (Komodo stack) on a host reachable on the tailnet, with
  `INFLUXDB_*` set to the central InfluxDB and `sources.yaml` listing each source
  against its **existing** bucket (so current dashboards keep working).
- Each source has an API key; its `sha256` is in `sources.yaml`
  (`printf '%s' 'THE-API-KEY' | sha256sum`).

## 1. Per boat — deploy the agent (parallel run)
1. On the boat, create the `sync-agent` stack `.env`: `API_URL` = the API's
   tailnet name, `API_KEY` = this boat's key, `LOCAL_INFLUX_*` = the boat's
   local InfluxDB and its existing bucket. Set `BACKFILL_FLOOR` to cover the
   current gap (e.g. `-30d`) for the first run.
2. `docker compose up -d --build`.
3. Watch logs: `docker logs -f boat-sync-agent` — expect "window(s) forwarded".

## 2. Verify parity
- Central InfluxDB point count for the boat's bucket should climb to match local.
- Spot-check timestamps/measurements in Grafana against the boat's local InfluxDB.
- Confirm `GET /v1/highwater` (with the boat's key) advances toward "now".

## 3. Retire the legacy pipeline (only after parity holds)
- On the legacy import host, disable the boat's cron line(s) for the old import script.
- Stop the boat's old export cron (`export_and_sync.sh`).
- Keep the old scripts in place (disabled) as a fallback for one season.

## 4. Rollback
- Re-enable the legacy cron lines; stop the `sync-agent` stack. Because writes
  are idempotent, running both briefly is safe.
