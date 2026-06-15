# ingress-api/tests/test_registry.py
import hashlib, textwrap, pytest
from app.registry import load_registry, SourceRegistry, Source

def _sha(k): return hashlib.sha256(k.encode()).hexdigest()

@pytest.fixture
def reg(tmp_path):
    p = tmp_path / "sources.yaml"
    p.write_text(textwrap.dedent(f"""
        sources:
          site-a:
            bucket: bucket_a
            tags: {{source: site_a}}
            key_sha256: "{_sha('key-a')}"
          site-b:
            bucket: bucket_b
            tags: {{source: site_b}}
            key_sha256: "{_sha('key-b')}"
    """))
    return load_registry(str(p))

def test_authenticate_returns_matching_source(reg):
    s = reg.authenticate("key-a")
    assert isinstance(s, Source)
    assert s.id == "site-a"
    assert s.bucket == "bucket_a"
    assert s.tags == {"source": "site_a"}

def test_authenticate_wrong_key_returns_none(reg):
    assert reg.authenticate("nope") is None

def test_authenticate_empty_key_returns_none(reg):
    assert reg.authenticate("") is None
    assert reg.authenticate(None) is None

def test_load_registry_rejects_duplicate_key(tmp_path):
    p = tmp_path / "s.yaml"
    same = _sha("dup")
    p.write_text(textwrap.dedent(f"""
        sources:
          a: {{bucket: b1, tags: {{}}, key_sha256: "{same}"}}
          b: {{bucket: b2, tags: {{}}, key_sha256: "{same}"}}
    """))
    with pytest.raises(ValueError, match="duplicate"):
        load_registry(str(p))

def test_authenticate_unregistered_but_hashlike_key_returns_none(reg):
    # A plausible 64-hex-char key that simply isn't registered (realistic rejection path)
    assert reg.authenticate("0" * 64) is None
