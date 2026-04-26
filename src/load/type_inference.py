"""Column type inference.

Applied to a list of *raw string* values (we never coerce data — the original
text is always preserved in `rows`). Inference is conservative:

- 95% of non-empty values must match a type to be classified as that type.
- Bool requires actual literals (true/false/yes/no/да/нет/T/F); plain 0/1
  is NOT bool — it's almost always numeric (flags, counts).
- Date detection avoids `pd.to_datetime(format="mixed")` for plain numbers
  (which would happily parse "7" as 1970-01-01 00:00:07). We require either
  a date-like character (separator) or a 4-digit year prefix/suffix.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

import pandas as pd

from src.models import ColumnType

# Threshold: how many non-empty values must match a type for that type to win.
TYPE_INFERENCE_THRESHOLD = 0.95

_BOOL_TRUE = {"true", "yes", "y", "да", "д", "истина"}
_BOOL_FALSE = {"false", "no", "n", "нет", "н", "ложь"}
_BOOL_LITERALS = _BOOL_TRUE | _BOOL_FALSE

# Heuristic: a candidate date string must contain at least one of these markers.
# Pure digits like "7" or "20240101" must NOT be classified as dates.
_DATE_MARKER_RE = re.compile(
    r"""
    [-/.]                       # date separator
    | \s\d{1,2}:\d{2}           # time component
    | \b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b
    | \b(?:янв|фев|мар|апр|мая|июн|июл|авг|сен|окт|ноя|дек)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _to_clean_strings(values: Iterable[object]) -> list[str]:
    out: list[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        out.append(s)
    return out


def _is_bool_literal(s: str) -> bool:
    return s.lower() in _BOOL_LITERALS


def _looks_like_date(s: str) -> bool:
    if not _DATE_MARKER_RE.search(s):
        return False
    # Reject very short or numeric-looking strings without real date markers.
    return len(s) >= 6


def _try_parse_number(s: str) -> bool:
    # Allow comma decimal separator (locale-dependent in RU).
    candidate = s.replace("\u00a0", "").replace(" ", "")
    if "," in candidate and "." not in candidate:
        candidate = candidate.replace(",", ".")
    # Reject leading-zero strings that look like codes/IDs (e.g. "007").
    # We still accept zero, "0.5", "-0", etc.
    try:
        float(candidate)
    except ValueError:
        return False
    return True


def _try_parse_date(s: str) -> tuple[bool, bool]:
    """Returns (is_date, has_time)."""
    if not _looks_like_date(s):
        return False, False
    try:
        ts = pd.to_datetime(s, errors="raise", dayfirst=False)
    except Exception:
        try:
            ts = pd.to_datetime(s, errors="raise", dayfirst=True)
        except Exception:
            return False, False
    if pd.isna(ts):
        return False, False
    has_time = bool(ts.hour or ts.minute or ts.second or ts.microsecond)
    return True, has_time


def infer_column_type(values: Sequence[object]) -> ColumnType:
    """Infer a column type from a sequence of raw values."""
    cleaned = _to_clean_strings(values)
    if not cleaned:
        return ColumnType.empty

    n = len(cleaned)
    threshold = max(1, int(n * TYPE_INFERENCE_THRESHOLD))

    bool_count = sum(1 for s in cleaned if _is_bool_literal(s))
    if bool_count >= threshold:
        # Require both true-like and false-like literals to be present —
        # otherwise it's a constant that we'd rather see as string.
        seen_lower = {s.lower() for s in cleaned}
        if seen_lower & _BOOL_TRUE and seen_lower & _BOOL_FALSE:
            return ColumnType.bool

    number_count = sum(1 for s in cleaned if _try_parse_number(s))
    if number_count >= threshold:
        return ColumnType.number

    date_count = 0
    datetime_count = 0
    for s in cleaned:
        ok, has_time = _try_parse_date(s)
        if ok:
            date_count += 1
            if has_time:
                datetime_count += 1
    if date_count >= threshold:
        return ColumnType.datetime if datetime_count >= threshold // 2 else ColumnType.date

    # Mixed: at least two of {numeric, date, string} present in non-trivial amounts.
    string_count = n - number_count - date_count
    signals = sum(1 for c in (number_count, date_count, string_count) if c >= max(1, n // 5))
    if signals >= 2:
        return ColumnType.mixed

    return ColumnType.string


def numeric_stats(values: Sequence[object]) -> tuple[float | None, float | None, float | None]:
    """Return (min, max, avg) for values that parse as numbers."""
    nums: list[float] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        candidate = s.replace("\u00a0", "").replace(" ", "")
        if "," in candidate and "." not in candidate:
            candidate = candidate.replace(",", ".")
        try:
            nums.append(float(candidate))
        except ValueError:
            continue
    if not nums:
        return None, None, None
    return min(nums), max(nums), sum(nums) / len(nums)


def date_stats(values: Sequence[object]) -> tuple[str | None, str | None]:
    """Return (min_date_iso, max_date_iso) for values that parse as dates."""
    dts: list[pd.Timestamp] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        ok, _ = _try_parse_date(s)
        if not ok:
            continue
        try:
            dts.append(pd.to_datetime(s, errors="raise"))
        except Exception:
            try:
                dts.append(pd.to_datetime(s, errors="raise", dayfirst=True))
            except Exception:
                continue
    if not dts:
        return None, None
    return min(dts).isoformat(), max(dts).isoformat()


_UNITS_RE = re.compile(r"""[,(]\s*([^,)]{1,15})\s*\)?$""")


def split_name_and_units(raw_name: str) -> tuple[str, str | None]:
    """Extract a units suffix from a header like "Цена, руб." or "Weight (kg)".

    Returns (clean_name, units_or_None).
    """
    s = str(raw_name).strip()
    if not s:
        return s, None
    m = _UNITS_RE.search(s)
    if not m:
        return s, None
    units = m.group(1).strip().rstrip(".")
    if not units or len(units) > 15:
        return s, None
    # Accept short tokens (likely units), reject long phrases.
    if len(units.split()) > 2:
        return s, None
    clean = _UNITS_RE.sub("", s).strip().rstrip(",").strip()
    if not clean:
        return s, None
    return clean, units
