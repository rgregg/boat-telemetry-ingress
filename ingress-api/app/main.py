import gzip
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.concurrency import run_in_threadpool
from app.config import load_settings
from app.registry import load_registry, Source
from app.influx import InfluxClient
from app.lp import inject_tags_into_lp

def create_app(influx: InfluxClient | None = None) -> FastAPI:
    settings = load_settings()
    registry = load_registry(settings.sources_file)
    influx = influx or InfluxClient(settings.influx_url, settings.influx_token, settings.influx_org)
    app = FastAPI(title="boat-telemetry-ingress")

    def auth(authorization: str | None = Header(default=None)) -> Source:
        key = None
        if authorization and authorization.lower().startswith("bearer "):
            key = authorization[7:]
        src = registry.authenticate(key)
        if src is None:
            raise HTTPException(status_code=401, detail="invalid api key")
        return src

    @app.post("/v1/ingest")
    async def ingest(request: Request, src: Source = Depends(auth)):
        body = await request.body()
        if request.headers.get("content-encoding", "").lower() == "gzip":
            # Tailnet-only + authenticated callers, so an unbounded decompression
            # (gzip-bomb) is an accepted risk; no size cap here.
            try:
                body = gzip.decompress(body)
            except (gzip.BadGzipFile, EOFError, OSError) as exc:
                raise HTTPException(status_code=400, detail="invalid gzip body") from exc
        try:
            text = body.decode("utf-8", errors="strict").strip()
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=400, detail="body must be utf-8") from exc
        if not text:
            raise HTTPException(status_code=400, detail="empty body")
        lp = inject_tags_into_lp(text, src.tags).encode("utf-8")
        try:
            await run_in_threadpool(influx.write, src.bucket, lp)
        except Exception:
            raise HTTPException(status_code=502, detail="influxdb write failed")
        return {"written": True}

    @app.get("/v1/highwater")
    def highwater(src: Source = Depends(auth)):
        try:
            hw = influx.highwater(src.bucket)
        except Exception:
            raise HTTPException(status_code=502, detail="influxdb query failed")
        return {"highwater": hw.isoformat() if hw else None}

    @app.get("/health")
    def health():
        return {"status": "ok", "influxdb": influx.health()}

    return app
