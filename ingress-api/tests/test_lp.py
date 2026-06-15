from app.lp import inject_tags_into_lp

def test_inject_into_line_with_existing_tags():
    out = inject_tags_into_lp("cpu,host=a load=1 100", {"source": "site_a"})
    assert out == "cpu,host=a,source=site_a load=1 100"

def test_inject_into_line_with_no_tags():
    out = inject_tags_into_lp("cpu load=1 100", {"source": "site_a"})
    assert out == "cpu,source=site_a load=1 100"

def test_inject_escapes_tag_key_and_value():
    out = inject_tags_into_lp("m f=1 1", {"a b": "c,d"})
    assert out == r"m,a\ b=c\,d f=1 1"

def test_inject_respects_escaped_space_in_measurement():
    # measurement "we ll" has an escaped space; first UNescaped space precedes fields
    out = inject_tags_into_lp(r"we\ ll f=1 1", {"s": "x"})
    assert out == r"we\ ll,s=x f=1 1"

def test_inject_multiple_lines_and_skips_blank():
    text = "a f=1 1\n\nb f=2 2\n"
    out = inject_tags_into_lp(text, {"s": "x"})
    assert out == "a,s=x f=1 1\nb,s=x f=2 2"

def test_inject_no_tags_is_passthrough():
    assert inject_tags_into_lp("a f=1 1", {}) == "a f=1 1"
