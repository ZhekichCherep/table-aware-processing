"""Demo runner.

Process all .csv / .xlsx files in an input directory and write JSON artifacts
to an output directory.

Examples:
    python run_demo.py
    python run_demo.py --input examples --output output
    python run_demo.py --input my_files --max-rows-per-chunk 100
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from src.chunking.table_chunker import ChunkSettings
from src.load.table_parser import export_artifacts


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", "-i", type=Path, default=Path("examples"))
    ap.add_argument("--output", "-o", type=Path, default=Path("output"))
    ap.add_argument("--max-rows-per-chunk", type=int, default=200)
    ap.add_argument("--max-cells-per-chunk", type=int, default=4000)
    ap.add_argument("--preview-rows", type=int, default=20)
    return ap.parse_args()


def _write_text_projections(chunks_path: Path) -> Path:
    out = chunks_path.with_suffix(".text_projection.txt")
    blocks: list[str] = []
    with chunks_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            if not line.strip():
                continue
            item = json.loads(line)
            chunk_id = item.get("chunk_id", "unknown_chunk")
            projection = item.get("text_projection", "")
            blocks.append(f"=== {chunk_id} ===\n{projection}")
    out.write_text("\n\n".join(blocks), encoding="utf-8")
    return out


def main() -> int:
    args = _parse_args()

    if not args.input.exists():
        print(
            f"Input directory not found: {args.input}\n"
            f"Hint: run `python gen_examples.py` to generate demo files first.",
            file=sys.stderr,
        )
        return 1

    settings = ChunkSettings(
        max_rows_per_chunk=args.max_rows_per_chunk,
        max_cells_per_chunk=args.max_cells_per_chunk,
        preview_rows_in_text_projection=args.preview_rows,
    )

    files = sorted(p for p in args.input.iterdir() if p.is_file() and p.suffix.lower() in {".csv", ".xlsx"})
    if not files:
        print(f"No .csv or .xlsx files found in {args.input}", file=sys.stderr)
        return 1

    print(f"Found {len(files)} file(s) in {args.input}:")
    for file in files:
        print(f"  - {file.name}")

    print("\nProcessing...")
    for file in files:
        t0 = time.perf_counter()
        artifacts = export_artifacts(file, output_dir=args.output, settings=settings)
        text_projection_path = _write_text_projections(Path(artifacts["chunks"]))
        dt = time.perf_counter() - t0
        print(f"\n{file.name}  ({dt:.2f}s)")
        print(f"  normalized: {artifacts['normalized']}")
        print(f"  chunks:     {artifacts['chunks']}")
        print(f"  profile:    {artifacts['profile']}")
        print(f"  projection: {text_projection_path}")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
