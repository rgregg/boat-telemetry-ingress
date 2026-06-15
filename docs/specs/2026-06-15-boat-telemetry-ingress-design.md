# Boat Telemetry Ingress — Design

**Date:** 2026-06-15
**Status:** Approved (design); implementation not started

## Problem

Remote sites (boats) collect Signal K marine telemetry into a **local InfluxDB**
and need it relayed to a **central InfluxDB** in the home lab for dashboards and
alerts. The existing pipeline does this by reading InfluxDB's raw on-disk
TSM/WAL files with an offline export tool, `rsync`-ing gzipped line protocol over
a VPN, and replaying it with shell scripts driven by cron over SSH.

This is fragile and hard to operate:

- The InfluxDB destination (URL/host) is duplicated into every import script. A
  stale address in one script silently sent all writes to a dead host and a
  boat's dashboard was blank for ~7 months before anyone noticed.
- Recovery depends on SSH access and hand-run shell scripts; it is hard to
  reproduce or stand back up.
- There is no per-source identity, validation, or central configuration.

## Goals

- **One place** to hold InfluxDB credentials and the source→bucket mapping.
- **Guaranteed no-loss with automatic backfill** across connectivity gaps
  (cellular drops, flaky marina wifi — minutes to hours; sites are also offline
  for long seasonal stretches).
- **Reproducible**: containerized, declarative config, no bespoke SSH/shell
  recovery steps.
- **Multi-source / generic**: built for the two boats now, but adding a source
  (another boat, the cabin, future sensors) is a config entry, not a code change.
- Remote sites never hold InfluxDB tokens and never learn InfluxDB's address.

## Non-goals

- Replacing Signal K or the site-local InfluxDB.
- Real-time streaming below the chosen poll interval.
- Public internet exposure of the API (tailnet only — see Security).
- Stale-data alerting (sites are intentionally offline most of the year).

## Architecture

```
┌─ REMOTE SITE (per vessel; tailnet node) ─────┐         ┌─ HOME (tailnet; near InfluxDB) ──────────────┐
│  local InfluxDB ──query since watermark──▶   │  HTTPS  │                                               │
│                            boat-sync-agent ──┼─────────┼─▶ boat-ingress-api ──write──▶ central InfluxDB │
│                         (stateless loop)     │ Bearer  │   (holds the ONLY InfluxDB credentials)        │
└───────────────────────────────────────────────┘  key  └────────────────────────────────────────────────┘
```

Two components, both built from this repo:

### `boat-ingress-api` (home)

- **Stack:** Python 3.12-alpine, FastAPI + uvicorn, non-root, minimal pinned
  deps. Mirrors the home lab's existing custom-service convention (single small
  app, env-var config, `build: .` compose, deployed as a Komodo stack).
- **Holds the single InfluxDB connection** (`INFLUXDB_URL`, `INFLUXDB_TOKEN`,
  `INFLUXDB_ORG`) via environment.
- **Source registry** (data-driven, see below) maps `source_id` →
  `{api_key hash, bucket, default tags}`.
- **Location-independent.** Because it's a container whose only dependencies
  are `INFLUXDB_URL` + token (via env) and network reachability to InfluxDB and
  the tailnet, it can run on any host — it does not need to be co-located with
  InfluxDB. Its real address/token are deployment config, not in this repo.

#### Endpoints

| Method & path | Auth | Purpose |
|---------------|------|---------|
| `GET /v1/highwater` | Bearer (per-source key) | Returns the latest timestamp the central InfluxDB already holds **for this source** (max `_time` over the source's bucket, filtered by the source's identifying tag). The agent's resume point. |
| `POST /v1/ingest` | Bearer (per-source key) | Body = InfluxDB line protocol (optionally gzip, `Content-Encoding: gzip`). Validates, applies the source's default tags, writes to the source's bucket. Returns 2xx only after a durable InfluxDB write. |
| `GET /health` | none | Liveness + central InfluxDB reachability. |

#### Responses / status codes

- `401` — missing/invalid API key.
- `400` — malformed line protocol.
- `2xx` — data durably written (ingest) / result returned (highwater).
- `502` — central InfluxDB write or query failed → the agent retries safely
  (no data is lost because the agent never advances past unconfirmed data).

### `boat-sync-agent` (remote site)

- **Stack:** Python 3.12-alpine, a `monitor.py`-style loop, non-root. One
  identical, parameterized container per site.
- **Stateless** — no local checkpoint file. The home side is the source of
  truth for "where am I caught up to," so the agent is restart- and
  reimage-safe.
- **Config (env):** `API_URL`, `API_KEY`, `SOURCE_ID`, `LOCAL_INFLUX_URL`,
  `LOCAL_INFLUX_TOKEN`, `LOCAL_INFLUX_ORG`, `LOCAL_INFLUX_BUCKET`,
  `POLL_INTERVAL`, `WINDOW`, `OVERLAP`, `BACKFILL_FLOOR`.

## Data flow & the no-loss guarantee

Each cycle the agent:

1. `GET /v1/highwater` → latest timestamp the central InfluxDB has for this
   source (or empty if none yet).
2. Compute `start = highwater − OVERLAP` (or `BACKFILL_FLOOR` if no highwater).
3. Query local InfluxDB for `_time > start`, in **bounded windows** of `WINDOW`
   (e.g. 6 h) so payloads stay small over cellular. Loop the windows within a
   cycle until caught up to "now."
4. Serialize each window to line protocol, gzip, `POST /v1/ingest`.
5. On `2xx` → proceed to the next window; the central highwater has advanced.
   On any failure → stop, log, back off, retry next cycle.

**Why this is no-loss with automatic backfill:**

- The **local InfluxDB is the durable buffer** — it already retains everything,
  so nothing depends on the agent staying up or holding a queue.
- The central **highwater only advances when data is confirmed written**
  centrally; the agent always resumes from the last confirmed point.
- After any outage (minutes or months), the agent resumes from the highwater and
  paginates forward until current — no gap, no manual backfill.
- InfluxDB writes are **idempotent** on `(measurement, tag set, field,
  timestamp)`, so the small `OVERLAP` re-send is harmless and covers
  per-measurement write lag at the boundary.

### Watermark assumption (documented trade-off)

The highwater is a single max `_time` per source. If one measurement lags others
by more than `OVERLAP`, a naive max could skip it. Signal K writes are
near-real-time per path, so a modest `OVERLAP` (e.g. 10 min) covers normal lag.
This is the standard pragmatic watermark approach; a stricter per-measurement
watermark is possible later if a lagging series is observed, but is out of scope
for v1.

## Source registry

A data-driven YAML config mounted into the API container. Adding a source is a
config entry plus a provisioned key — no code change.

```yaml
# sources.example.yaml  (the real sources.yaml is untracked deployment config;
# bucket names, tags, and key hashes are all placeholders here)
sources:
  site-a:
    bucket: "<bucket-for-site-a>"      # an existing bucket → dashboards keep working
    tags: { source: "<tag-value>" }
    key_sha256: "<sha256 of the api key>"
  site-b:
    bucket: "<bucket-for-site-b>"
    tags: { source: "<tag-value>" }
    key_sha256: "<sha256 of the api key>"
```

Real bucket names, tag values, and keys live only in the untracked
`sources.yaml` at deploy time — never in this repo.

## Auth & exposure (Security)

- **Per-source API keys** (bearer). Stored **hashed** (sha256) in the registry;
  compared in constant time. Keys identify the source → bucket + tags.
- **Tailnet-only.** The API is not exposed to the public internet; remote sites
  reach it over the existing mesh VPN (already WireGuard-encrypted), so plain
  HTTP between agent and API is acceptable. Optional TLS termination at the
  existing reverse proxy is possible but not required.
- This design **structurally prevents the original failure**: InfluxDB's address
  and token live in exactly one container; remote sites can neither point at a
  stale InfluxDB nor leak an InfluxDB token.

## Error handling & observability

- **Agent:** every error (API down, local InfluxDB down, network) → log, back
  off, retry; never advances past unconfirmed data; bounded backfill windows.
- **API:** structured logs; `401`/`400`/`502` as above; `/health` checks central
  InfluxDB. Tracks **last-ingest age per source**, exposable to a dashboard
  later (no stale alerting, by design — seasonal sites).

## Deployment & cutover

- **Home:** `boat-ingress-api/` (Dockerfile + compose + app) deployed as a
  Komodo stack on any host that can reach InfluxDB and the tailnet (it is not
  tied to the InfluxDB host).
- **Site:** `boat-sync-agent/` (Dockerfile + compose + app), a per-site compose
  stack alongside Signal K.
- **Cutover (parallel-run, then retire old):**
  1. Stand up the API; register both sources against their **existing buckets**.
  2. Deploy the agent on **Happy Orca** (currently online); run alongside the old
     pipeline and verify parity (same points/timestamps/tags).
  3. Deploy on **Little Orca** when it next comes online.
  4. Retire the old export/rsync/cron/shell import pipeline once verified; keep
     it available as a fallback until confident.

## Testing

- **API:** unit tests for auth, registry lookup, line-protocol validation,
  highwater query, and ingest write — against a throwaway InfluxDB container.
- **Agent:** unit tests for the highwater → window → post loop, backfill
  pagination, and failure-without-advance (no watermark movement on error).
- **End-to-end:** compose up agent + API + InfluxDB; push synthetic Signal K
  data into a local InfluxDB; assert it lands in the correct bucket with correct
  tags and timestamps; simulate an outage and assert clean backfill.

## Open items for the implementation plan

- Exact local-InfluxDB query (Flux) and line-protocol reserialization, preserving
  original measurement/tag/field/timestamp.
- Concrete `WINDOW` / `OVERLAP` / `POLL_INTERVAL` / `BACKFILL_FLOOR` defaults.
- Whether the agent loops to "now" within one cycle or one window per cycle.
- Pinned dependency versions and base image digests.
