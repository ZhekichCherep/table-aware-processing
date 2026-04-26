"""Top-level public API for the table-aware processing pipeline.

Exposes:
- `parse_table_file(path)` -> NormalizedDocument
- `export_artifacts(input_file, output_dir, settings)` -> dict with file paths
"""

from __future__ import annotations

import json
from pathlib import Path

from src.chunking.table_chunker import ChunkSettings, build_chunks
from src.load.csv_parser import parse_csv
from src.load.xlsx_parser import parse_xlsx
from src.models import NormalizedDocument


def parse_table_file(file_path: str | Path) -> NormalizedDocument:
    path = Path(file_path)
    ext = path.suffix.lower()
    if ext == ".csv":
        return parse_csv(path)
    if ext == ".xlsx":
        return parse_xlsx(path)
    raise ValueError(f"Unsupported format: {ext}")


def export_artifacts(
    input_file: str | Path,
    output_dir: str | Path = "output",
    settings: ChunkSettings | None = None,
) -> dict[str, str]:
    path = Path(input_file)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    normalized = parse_table_file(path)
    chunks = build_chunks(normalized, settings=settings)

    normalized_path = out_dir / f"{path.stem}.normalized.json"
    chunks_path = out_dir / f"{path.stem}.chunks.jsonl"
    profile_path = out_dir / f"{path.stem}.profile.json"

    _write_normalized(normalized, normalized_path)
    _write_chunks(chunks, chunks_path)
    _write_profile(normalized, profile_path)

    return {
        "normalized": str(normalized_path),
        "chunks": str(chunks_path),
        "profile": str(profile_path),
    }


def _write_normalized(doc: NormalizedDocument, out: Path) -> None:
    """Stream-friendly JSON writer.

    `model_dump_json` would serialize the entire document to a single in-memory
    string. For 100k-row stress files that's wasteful — we serialize regions
    individually and stream their `rows` array.
    """
    with out.open("w", encoding="utf-8") as fp:
        fp.write("{\n")
        fp.write(f'  "schema_version": {json.dumps(doc.schema_version)},\n')
        fp.write(f'  "source_file": {json.dumps(doc.source_file, ensure_ascii=False)},\n')
        fp.write(f'  "source_format": {json.dumps(doc.source_format)},\n')
        fp.write(f'  "processed_at": {json.dumps(doc.processed_at.isoformat())},\n')
        fp.write('  "sheets": [\n')
        for s_idx, sheet in enumerate(doc.sheets):
            fp.write("    {\n")
            fp.write(f'      "sheet_name": {json.dumps(sheet.sheet_name, ensure_ascii=False)},\n')
            fp.write('      "table_regions": [\n')
            for r_idx, region in enumerate(sheet.table_regions):
                _write_region(fp, region, indent="        ")
                fp.write(",\n" if r_idx < len(sheet.table_regions) - 1 else "\n")
            fp.write("      ]\n")
            fp.write("    }")
            fp.write(",\n" if s_idx < len(doc.sheets) - 1 else "\n")
        fp.write("  ]\n")
        fp.write("}\n")


def _write_region(fp, region, *, indent: str) -> None:
    """Write a single TableRegion in pretty form, with rows streamed."""
    payload = region.model_dump(mode="json")
    rows = payload.pop("rows")
    # Pretty-print everything except `rows` (which we stream at the end).
    head = json.dumps(payload, ensure_ascii=False, indent=2)
    head_lines = head.split("\n")
    indented = ("\n" + indent).join(head_lines)
    # Strip the closing brace; we'll add `,"rows": [...]}` ourselves.
    if not indented.endswith("}"):
        raise RuntimeError("Unexpected JSON head shape for TableRegion")
    fp.write(indent + indented[:-1])
    fp.write(',\n' + indent + '  "rows": [')
    for row_idx, row in enumerate(rows):
        fp.write("\n" + indent + "    " + json.dumps(row, ensure_ascii=False))
        if row_idx < len(rows) - 1:
            fp.write(",")
    if rows:
        fp.write("\n" + indent + "  ")
    fp.write("]\n" + indent + "}")


def _write_chunks(chunks, out: Path) -> None:
    with out.open("w", encoding="utf-8") as fp:
        for chunk in chunks:
            fp.write(json.dumps(chunk.model_dump(mode="json"), ensure_ascii=False))
            fp.write("\n")


def _write_profile(doc: NormalizedDocument, out: Path) -> None:
    tables: list[dict[str, object]] = []
    for sheet in doc.sheets:
        for table in sheet.table_regions:
            tables.append(
                {
                    "sheet_name": sheet.sheet_name,
                    "table_id": table.table_id,
                    "source_ref": table.source_ref.model_dump(mode="json"),
                    "profile": table.profile.model_dump(mode="json"),
                }
            )
    payload = {
        "schema_version": "v1",
        "source_file": doc.source_file,
        "source_format": doc.source_format,
        "tables": tables,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
