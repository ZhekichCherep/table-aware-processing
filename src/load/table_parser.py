from __future__ import annotations

import json
from pathlib import Path

from src.chunking.table_chunker import ChunkSettings, build_chunks
from src.load.csv_parser import parse_csv
from src.load.models import NormalizedDocument
from src.load.xlsx_parser import parse_xlsx


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
    profile = _collect_profile(normalized)

    normalized_path = out_dir / f"{path.stem}.normalized.json"
    chunks_path = out_dir / f"{path.stem}.chunks.jsonl"
    profile_path = out_dir / f"{path.stem}.profile.json"

    normalized_path.write_text(
        normalized.model_dump_json(indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    with chunks_path.open("w", encoding="utf-8") as fp:
        for chunk in chunks:
            fp.write(json.dumps(chunk.model_dump(mode="json"), ensure_ascii=False) + "\n")
    profile_path.write_text(
        json.dumps(profile, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "normalized": str(normalized_path),
        "chunks": str(chunks_path),
        "profile": str(profile_path),
    }


def _collect_profile(doc: NormalizedDocument) -> dict[str, object]:
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
    return {
        "schema_version": "v1",
        "source_file": doc.source_file,
        "source_format": doc.source_format,
        "tables": tables,
    }
