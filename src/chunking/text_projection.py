from __future__ import annotations


def build_text_projection(
    sheet_name: str,
    table_id: str,
    source_ref_a1: str | None,
    header_context: list[str],
    row_start: int,
    row_end: int,
    rows: list[dict[str, object]],
) -> str:
    preview_lines: list[str] = []
    for row in rows[:5]:
        preview_lines.append(" | ".join(str(row.get(h, "")) for h in header_context))
    preview = "\n".join(preview_lines)
    return (
        f"Sheet: {sheet_name}\n"
        f"Table: {table_id}\n"
        f"SourceRef: {source_ref_a1 or '-'}\n"
        f"Columns: {', '.join(header_context)}\n"
        f"Rows {row_start}-{row_end}:\n"
        f"{preview}"
    )
