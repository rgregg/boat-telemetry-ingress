# boat-telemetry-ingress

Durable, no-loss telemetry relay from remote sites (boats, and other locations
later) into a central InfluxDB.

Each remote site already runs a local InfluxDB that retains everything. A small
**sync agent** on the site forwards new data to a central **ingress API**, which
is the single place that holds InfluxDB credentials and the source→bucket
mapping. Sites never hold InfluxDB tokens and never learn InfluxDB's address —
they only know the API URL and their own API key.

## Why

The previous pipeline (raw on-disk InfluxDB export → `rsync` → shell-script
import via cron over SSH) was fragile and duplicated the InfluxDB destination
across every script — a stale address there silently blackholed a boat's data
for ~7 months. This replaces it with two small containers and one central
config point.

## Components

| Component | Runs on | Role |
|-----------|---------|------|
| `boat-ingress-api` | Home (tailnet, near InfluxDB) | HTTP API: auth per source, validate, write to InfluxDB, report per-source high-water mark. Holds the only InfluxDB credentials. |
| `boat-sync-agent`  | Each remote site | Stateless loop: ask the API where it's caught up to, query local InfluxDB forward, POST. Local InfluxDB is the durable buffer. |

## Status

Design phase. See [`docs/specs/`](docs/specs/) for the architecture spec. No
implementation yet.

## Security note (public repo)

Never commit API keys, InfluxDB tokens, tailnet hostnames/IPs, or `.env` /
`sources.yaml` files. Use the `*.example` templates and keep real values in
untracked env files. See `.gitignore`.
