# ingress-api/app/registry.py
import hashlib, hmac
from dataclasses import dataclass
import yaml

@dataclass(frozen=True)
class Source:
    id: str
    bucket: str
    tags: dict
    key_sha256: str

class SourceRegistry:
    def __init__(self, sources: list[Source]):
        self._sources = sources
        seen = set()
        for s in sources:
            if s.key_sha256 in seen:
                raise ValueError(f"duplicate key_sha256 for source {s.id}")
            seen.add(s.key_sha256)

    def authenticate(self, api_key: str | None) -> Source | None:
        if not isinstance(api_key, str) or not api_key:
            return None
        digest = hashlib.sha256(api_key.encode()).hexdigest()
        result = None
        for s in self._sources:
            if hmac.compare_digest(digest, s.key_sha256):
                result = s
        return result

def load_registry(path: str) -> SourceRegistry:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    sources = []
    for sid, cfg in (data.get("sources") or {}).items():
        try:
            sources.append(Source(
                id=sid,
                bucket=cfg["bucket"],
                tags=dict(cfg.get("tags") or {}),
                key_sha256=cfg["key_sha256"],
            ))
        except KeyError as exc:
            raise ValueError(f"source '{sid}' is missing required field {exc}") from exc
    return SourceRegistry(sources)
