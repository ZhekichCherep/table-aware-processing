from __future__ import annotations

import json
from pathlib import Path

from src.load.table_parser import ChunkSettings, export_artifacts


def main() -> None:
    input_dir = Path("test_data")
    output_dir = Path("output")
    settings = ChunkSettings(max_rows_per_chunk=200, max_cells_per_chunk=4000, preview_rows_in_text_projection=20)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    supported = {".csv", ".xlsx"}
    files = [p for p in sorted(input_dir.iterdir()) if p.is_file() and p.suffix.lower() in supported]
    if not files:
        print("No .csv or .xlsx files found in test_data")
        return

    print(f"Found {len(files)} files in {input_dir}:")
    for file in files:
        print(f"- {file.name}")

    print("\nProcessing...")
    for file in files:
        artifacts = export_artifacts(file, output_dir=output_dir, settings=settings)
        text_projection_path = _write_text_projections(Path(artifacts["chunks"]))
        print(f"\n{file.name}")
        print(f"  normalized: {artifacts['normalized']}")
        print(f"  chunks:     {artifacts['chunks']}")
        print(f"  profile:    {artifacts['profile']}")
        print(f"  projection: {text_projection_path}")

    print("\nDone.")

def _write_text_projections(chunks_path: Path) -> Path:
    output_path = chunks_path.with_suffix(".text_projection.txt")
    blocks: list[str] = []

    with chunks_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            if not line.strip():
                continue
            item = json.loads(line)
            chunk_id = item.get("chunk_id", "unknown_chunk")
            projection = item.get("text_projection", "")
            blocks.append(f"=== {chunk_id} ===\n{projection}")

    output_path.write_text("\n\n".join(blocks), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    main()
