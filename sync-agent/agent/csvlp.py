# sync-agent/agent/csvlp.py
from datetime import datetime, timezone

_RESERVED = {"", "result", "table", "_start", "_stop", "_time", "_value",
             "_field", "_measurement"}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

def _esc_tag(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")

def _esc_meas(s: str) -> str:
    return s.replace("\\", "\\\\").replace(",", r"\,").replace(" ", r"\ ")

def _to_ns(rfc3339: str) -> int:
    # datetime caps fractional seconds at microseconds, so any sub-microsecond
    # precision in _time is dropped (marine telemetry is far coarser, so fine).
    # Integer timedelta arithmetic avoids float64 rounding at nanosecond magnitudes.
    dt = datetime.fromisoformat(rfc3339.replace("Z", "+00:00"))
    td = dt - _EPOCH
    return td.days * 86_400 * 1_000_000_000 + td.seconds * 1_000_000_000 + td.microseconds * 1_000

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
        if not rec.get("_value"):          # skip null/empty values (InfluxDB drops nulls)
            continue
        meas = _esc_meas(rec["_measurement"])
        tags = "".join(
            f",{_esc_tag(col)}={_esc_tag(rec[col])}"
            for col in header
            if col not in _RESERVED and rec.get(col)
        )
        vtype = datatypes[header.index("_value")] if datatypes else "double"
        value = _fmt_value(rec["_value"], vtype)
        ns = _to_ns(rec["_time"])
        lines.append(f"{meas}{tags} {_esc_tag(rec['_field'])}={value} {ns}")
    return "\n".join(lines)
