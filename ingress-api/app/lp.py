def _escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")

def _split_unescaped(line: str, sep: str) -> int:
    """Index of the first unescaped `sep` char, or -1."""
    i = 0
    while i < len(line):
        c = line[i]
        if c == "\\":
            i += 2
            continue
        if c == sep:
            return i
        i += 1
    return -1

def _inject_one(line: str, rendered: str) -> str:
    space = _split_unescaped(line, " ")
    head = line if space == -1 else line[:space]
    rest = "" if space == -1 else line[space:]
    return head + rendered + rest

def inject_tags_into_lp(lp: str, tags: dict) -> str:
    if not tags:
        return "\n".join(l for l in lp.splitlines() if l.strip())
    rendered = "".join(f",{_escape(k)}={_escape(v)}" for k, v in tags.items())
    out = [_inject_one(l, rendered) for l in lp.splitlines() if l.strip()]
    return "\n".join(out)
