# ingress-api

Central HTTP API that authenticates each source by API key, applies the
source's default tags, and writes line protocol to that source's InfluxDB
bucket. Holds the only InfluxDB credentials.

## Configure
1. `cp .env.example .env` and set `INFLUXDB_TOKEN`.
2. `cp sources.example.yaml sources.yaml` and add each source. Generate a key
   hash with: `printf '%s' 'THE-API-KEY' | sha256sum`.

## Run
```bash
docker compose up -d --build
```

## Endpoints
- `GET /v1/highwater` (Bearer key) → `{"highwater": "<iso8601>"|null}`
- `POST /v1/ingest` (Bearer key, body=line protocol, optional `Content-Encoding: gzip`) → `{"written": true}`
- `GET /health` → `{"status":"ok","influxdb":true|false}`

## Test
```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && pytest
```
