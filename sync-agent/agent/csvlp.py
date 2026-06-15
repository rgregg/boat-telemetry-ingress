from datetime import datetime

_RESERVED = {"", "result", "table", "_start", "_stop", "_time", "_value",
             "_field", "_measurement"}

def _esc_tag(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")

def _esc_meas(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace(" ", r"\ ")

def _to_ns(rfc3339: str) -> int:
    dt = datetime.fromisoformat(rfc3339.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)

def _fmt_value(raw: str, datatype: str) -> str:
    if datatype in ("long", "unsignedLong"):
        return f"{int(raw)}i"
    if datatype == "boolean":
        return "true" if raw.lower() == "true" else "false"
    if datatype in ("double", "float"):
        return raw
    # string (or anything else) → quoted
    return '"' + raw.replace("\\", "\\\\").replace('"', '\\"') + '"'

def annotated_csv_to_lp(csv_text: str) -> str:
    datatypes: list[str] = []
    header: list[str] = []
    lines = []
    for raw in csv_text.splitlines():
        row = raw.rstrip("\r")
        if not row.strip():
            continue
        cells = row.split(",")
        if row.startswith("#datatype"):
            datatypes = cells
            continue
        if row.startswith("#"):
            continue
        if "_measurement" in cells:        # header row
            header = cells
            continue
        if not header:
            continue
        rec = dict(zip(header, cells))
        meas = _esc_meas(rec["_measurement"])
        tags = "".join(
            f",{_esc_tag(col)}={_esc_tag(rec[col])}"
            for col in header
            if col not in _RESERVED and rec.get(col)
        )
        vtype = datatypes[header.index("_value")] if datatypes else "double"
        value = _fmt_value(rec["_value"], vtype)
        ns = _to_ns(rec["_time"])
        lines.append(f"{meas}{tags} {rec['_field']}={value} {ns}")
    return "\n".join(lines)
