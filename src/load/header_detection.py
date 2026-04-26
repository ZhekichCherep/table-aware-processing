"""Header detection and column-name normalization.

For xlsx, we accept a list-of-lists representation of the first N rows of a
sheet (with merges already expanded by the parser) and decide:

- how many top rows belong to the header (`header_rows`),
- the resulting column names (joined by " / " across header rows).

For CSV we use a simpler heuristic: typically `header_rows = 1`, but we still
detect "no header" sheets and downgrade to synthetic names.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from src.load.type_inference import _try_parse_date, _try_parse_number, split_name_and_units

# Maximum number of top rows we ever consider as part of the header.
MAX_HEADER_ROWS = 5

_UNNAMED_RE = re.compile(r"^Unnamed:\s*\d+$", re.IGNORECASE)
_NORMALIZE_NON_WORD = re.compile(r"[^\w\d]+", re.UNICODE)


def _is_data_like(value: object) -> bool:
    """A 'data' cell is one that looks like a number or a date — not a label."""
    if value is None:
        return False
    s = str(value).strip()
    if s == "":
        return False
    if _try_parse_number(s):
        return True
    ok, _ = _try_parse_date(s)
    return ok


def _is_text_like(value: object) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    if s == "" or _UNNAMED_RE.match(s):
        return False
    return not _is_data_like(s)


def detect_header_rows(rows: Sequence[Sequence[object]], *, max_rows: int = MAX_HEADER_ROWS) -> int:
    """Return the number of top rows that form the header.

    Heuristic:
    1. The first row that is mostly data (>= 50% data-like cells) ends the header.
    2. The header is at least 1 row.
    3. We never return more than `min(max_rows, len(rows))`.
    """
    if not rows:
        return 0
    cap = min(max_rows, len(rows))
    for i in range(cap):
        row = rows[i]
        non_empty = [c for c in row if c is not None and str(c).strip() != ""]
        if not non_empty:
            # An empty row inside the candidate header zone — break here.
            return max(1, i)
        data_like = sum(1 for c in non_empty if _is_data_like(c))
        if i > 0 and data_like / len(non_empty) >= 0.5:
            return i
    return max(1, cap)


def build_column_names(
    header_rows: Sequence[Sequence[object]],
    column_count: int,
) -> list[tuple[str, str]]:
    """Combine multiple header rows into per-column (raw, normalized) names.

    Merged-cell expansion must be done before calling this — values are taken
    as-is. Empty cells are simply skipped during the join.
    """
    out: list[tuple[str, str]] = []
    seen: dict[str, int] = {}
    for col_idx in range(column_count):
        parts: list[str] = []
        for row in header_rows:
            if col_idx >= len(row):
                continue
            v = row[col_idx]
            if v is None:
                continue
            s = str(v).strip()
            if s == "" or _UNNAMED_RE.match(s):
                continue
            if parts and parts[-1] == s:
                continue
            parts.append(s)
        raw = " / ".join(parts) if parts else ""
        normalized = _normalize_name(raw, col_idx, seen)
        if not raw:
            raw = normalized
        out.append((raw, normalized))
    return out


def _normalize_name(raw: str, col_idx: int, seen: dict[str, int]) -> str:
    """Produce a stable, machine-friendly name; deduplicate within a header."""
    s = str(raw).strip()
    if not s or _UNNAMED_RE.match(s):
        candidate = f"col_{col_idx + 1}"
    else:
        clean, _units = split_name_and_units(s)
        lowered = clean.lower()
        normalized = _NORMALIZE_NON_WORD.sub("_", lowered).strip("_")
        candidate = normalized or f"col_{col_idx + 1}"
    if candidate in seen:
        seen[candidate] += 1
        return f"{candidate}_{seen[candidate]}"
    seen[candidate] = 1
    return candidate
