"""CSV parser.

Auto-detects encoding (utf-8-sig / cp1251 / ...) and separator (`,` / `;` /
`\\t` / `|`). Always reads as raw strings — type inference is a separate step
that never touches the original data.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from src.load.encoding_detection import detect_encoding, detect_separator
from src.load.header_detection import build_column_names, detect_header_rows
from src.load.regions import build_table_regions, looks_like_vertical_grid
from src.load.type_inference import split_name_and_units
from src.models import NormalizedDocument, SheetModel

CSV_SHEET_NAME = "csv_sheet"


def _read_raw_rows(path: Path, encoding: str, separator: str) -> list[list[str]]:
    rows: list[list[str]] = []
    with path.open("r", encoding=encoding, errors="replace", newline="") as fp:
        reader = csv.reader(fp, delimiter=separator)
        for row in reader:
            rows.append([("" if c is None else str(c)) for c in row])
    return rows


def _pad_rows(rows: list[list[str]], width: int) -> list[list[str]]:
    return [r + [""] * (width - len(r)) if len(r) < width else r[:width] for r in rows]


def parse_csv(
    file_path: str | Path,
    *,
    encoding: str | None = None,
    separator: str | None = None,
    top_n: int = 5,
) -> NormalizedDocument:
    path = Path(file_path)

    detected_encoding = encoding or detect_encoding(path)
    detected_separator = separator or detect_separator(path, detected_encoding)

    raw_rows = _read_raw_rows(path, detected_encoding, detected_separator)
    if not raw_rows:
        return NormalizedDocument(
            source_file=str(path),
            source_format="csv",
            processed_at=datetime.now(timezone.utc),
            sheets=[SheetModel(sheet_name=CSV_SHEET_NAME, table_regions=[])],
        )

    width = max(len(r) for r in raw_rows)
    raw_rows = _pad_rows(raw_rows, width)

    # Vertical (key/value) layouts have no real header — every row is data.
    is_vertical = looks_like_vertical_grid(raw_rows)
    if is_vertical:
        header_rows_count = 0
        raw_names = [f"col_{i + 1}" for i in range(width)]
        norm_names = list(raw_names)
        units: list[str | None] = [None] * width
        rows_dicts = [
            {norm_names[i]: cell for i, cell in enumerate(row)} for row in raw_rows
        ]
        # First data row sits at sheet row 1 (no header above it).
        row_offset = 1
    else:
        header_rows_count = detect_header_rows(raw_rows)
        header_block = raw_rows[:header_rows_count]
        data_rows_lists = raw_rows[header_rows_count:]

        name_pairs = build_column_names(header_block, width)
        raw_names = [r for r, _ in name_pairs]
        norm_names = [n for _, n in name_pairs]
        units = [split_name_and_units(r)[1] for r in raw_names]

        rows_dicts = [
            {norm_names[i]: cell for i, cell in enumerate(row)}
            for row in data_rows_lists
        ]
        row_offset = header_rows_count + 1

    extra_warnings = []
    if detected_encoding != "utf-8":
        extra_warnings.append(f"detected_encoding:{detected_encoding}")
    if detected_separator != ",":
        extra_warnings.append(f"detected_separator:{detected_separator!r}")

    regions = build_table_regions(
        sheet_name=CSV_SHEET_NAME,
        table_id_prefix=f"{path.stem}_t1",
        column_names_raw=raw_names,
        column_names_normalized=norm_names,
        column_units=units,
        rows=rows_dicts,
        header_rows=header_rows_count,
        row_offset=row_offset,
        col_start=1,
        col_end=max(1, width),
        range_a1_func=lambda rs, re_, cs, ce: None,
        extra_warnings=extra_warnings,
        top_n=top_n,
        orientation_hint="vertical" if is_vertical else None,
    )

    return NormalizedDocument(
        source_file=str(path),
        source_format="csv",
        processed_at=datetime.now(timezone.utc),
        sheets=[SheetModel(sheet_name=CSV_SHEET_NAME, table_regions=regions)],
    )
