from __future__ import annotations


def _is_noise_header(h: str) -> bool:
    s = h.strip()
    if s.lower().startswith("unnamed:"):
        return True
    if s.lower().startswith("col_"):
        return True
    return False


def build_text_projection(
    sheet_name: str,
    table_id: str,
    source_ref_a1: str | None,
    header_context: list[str],
    row_start: int,
    row_end: int,
    rows: list[dict[str, object]],
    *,
    preview_rows: int = 20,
) -> str:
    preview_lines: list[str] = []
    limit_rows = max(0, int(preview_rows))
    for idx, row in enumerate(rows[:limit_rows], start=row_start):
        items: list[str] = []
        for h in header_context:
            v = row.get(h, "")
            s = "" if v is None else str(v).strip()
            if s == "":
                continue
            if _is_noise_header(h):
                items.append(s)
            else:
                items.append(f"{h}: {s}")

        if not items:
            continue

        # Avoid extremely long lines on very wide sheets.
        limit = 12
        if len(items) > limit:
            omitted = len(items) - limit
            items = items[:limit] + [f"...(+{omitted} more)"]

        preview_lines.append(f"{idx}: " + " | ".join(items))
    preview = "\n".join(preview_lines)
    columns_line = ", ".join([h for h in header_context if not _is_noise_header(h)])
    if not columns_line:
        columns_line = "-"
    return (
        f"Sheet: {sheet_name}\n"
        f"Table: {table_id}\n"
        f"SourceRef: {source_ref_a1 or '-'}\n"
        f"Columns: {columns_line}\n"
        f"Rows {row_start}-{row_end}:\n"
        f"{preview}"
    )
