"""Plain-text projection of a chunk for full-text indexing."""

from __future__ import annotations


def _is_noise_header(name: str) -> bool:
    s = name.strip().lower()
    return s.startswith("col_") or s.startswith("unnamed:") or s.startswith("field_")


def _format_item(header: str, value: str) -> str:
    return value if _is_noise_header(header) else f"{header}: {value}"


def _collapse_runs(pairs: list[tuple[str, str]]) -> list[str]:
    """Run-length encode consecutive identical values.

    Merged-cell expansion at parse time produces many cells with the same
    value (e.g. a header label spanning 18 columns). The data layer must
    preserve that — but for full-text projection it's pure noise. This collapses
    runs in place so projections look like

        'Наименование контрагента: (×11) | АО АВТОПАРК ... (×24)'

    instead of repeating the same string a dozen+ times.
    """
    out: list[str] = []
    i = 0
    while i < len(pairs):
        h, v = pairs[i]
        j = i + 1
        while j < len(pairs) and pairs[j][1] == v:
            j += 1
        run_len = j - i
        rendered = _format_item(h, v)
        out.append(f"{rendered} (×{run_len})" if run_len > 1 else rendered)
        i = j
    return out


def build_text_projection(
    *,
    sheet_name: str,
    table_id: str,
    source_ref_a1: str | None,
    header_context: list[str],
    row_start: int,
    row_end: int,
    rows: list[dict[str, object]],
    preview_rows: int = 20,
) -> str:
    preview_lines: list[str] = []
    limit = max(0, int(preview_rows))
    for offset, row in enumerate(rows[:limit]):
        pairs: list[tuple[str, str]] = []
        for h in header_context:
            v = row.get(h, "")
            s = "" if v is None else str(v).strip()
            if s == "":
                continue
            pairs.append((h, s))
        if not pairs:
            continue

        items = _collapse_runs(pairs)
        items_limit = 12
        if len(items) > items_limit:
            omitted = len(items) - items_limit
            items = [*items[:items_limit], f"and {omitted} more values"]

        absolute_row = row_start + offset
        preview_lines.append(f"{absolute_row}: " + " | ".join(items))

    columns_line = ", ".join(h for h in header_context if not _is_noise_header(h))
    if not columns_line:
        columns_line = "-"

    return (
        f"Sheet: {sheet_name}\n"
        f"Table: {table_id}\n"
        f"SourceRef: {source_ref_a1 or '-'}\n"
        f"Columns: {columns_line}\n"
        f"Rows {row_start}-{row_end}:\n" + "\n".join(preview_lines)
    )
