# table-aware-processing

Table-aware processing for `.xlsx` and `.csv` with structure-preserving chunking
for RAG. This module replaces "flatten the table to text" pipelines with one
that keeps sheets, headers (including multi-row), units, ranges, and source
coordinates so retrieval can cite back to specific cells.

## What it does

- Reads `.xlsx` (via `openpyxl`) and `.csv` (with auto-detected encoding and
  separator) — raw values are never coerced; type inference runs side-by-side.
- Detects multi-row headers, expands merged cells, skips hidden rows/columns
  (with a warning), and splits regions on fully empty rows.
- Profiles each column: `% missing`, numeric `min/max/avg`, date `min/max`,
  top-N values for strings.
- Emits per-table warnings: `many_empty_rows`, `mixed_types_in_column:<name>`,
  `possible_totals_row`, `hidden_rows_present`, `hidden_columns_present`.
- Chunks tables by row ranges (not characters), with each chunk carrying
  header context and absolute sheet coordinates (`A1:K2000`-style).
- Streams large `normalized.json` to disk row-by-row to keep memory bounded.

## Requirements

- Python 3.10+

```bash
pip install -r requirements.txt          # runtime
pip install -r requirements-dev.txt      # + pytest, ruff
```

## Quick start

```bash
python gen_examples.py                    # creates examples/{small.csv,medium.xlsx,stress.xlsx}
python run_demo.py --input examples       # processes them into output/
pytest                                    # run the test suite
ruff check .                              # lint
```

CLI flags for the runner:

```bash
python run_demo.py --input my_files --output out --max-rows-per-chunk 100
```

## Output contract (schema v1)

For each input file the parser produces three artifacts:

- `*.normalized.json`  — full structured representation (streamed).
- `*.chunks.jsonl`     — one chunk per line, RAG-ready.
- `*.profile.json`     — per-table and per-column profile (subset of normalized).

Plus `*.text_projection.txt` from `run_demo.py` for human inspection.

### `normalized.json`

Top-level: `NormalizedDocument`.

```jsonc
{
  "schema_version": "v1",
  "source_file": "examples/medium.xlsx",
  "source_format": "xlsx",
  "processed_at": "2026-04-26T11:00:00+00:00",
  "sheets": [
    {
      "sheet_name": "Sales",
      "table_regions": [
        {
          "table_id": "medium_Sales_t1",
          "sheet_name": "Sales",
          "orientation": "horizontal",
          "header_rows": 2,
          "row_count": 5,
          "column_count": 5,
          "source_ref": {
            "sheet_name": "Sales",
            "range_a1": "A1:E7",
            "row_start": 1, "row_end": 7,
            "col_start": 1, "col_end": 5
          },
          "columns": [
            {"index": 0, "name_raw": "Region", "name_normalized": "region",
             "inferred_type": "string", "missing_pct": 0.0, "units": null},
            {"index": 1, "name_raw": "Q1 / Plan", "name_normalized": "q1_plan",
             "inferred_type": "number", "missing_pct": 0.0, "units": null}
          ],
          "rows": [
            {"region": "North", "q1_plan": "100", "q1_fact": "92",
             "q2_plan": "110", "q2_fact": "118"}
          ],
          "profile": { "...": "see profile.json" }
        }
      ]
    }
  ]
}
```

### `chunks.jsonl`

One `ChunkModel` per line:

```json
{
  "chunk_id": "medium:medium_Sales_t1:3-7",
  "source_file": "examples/medium.xlsx",
  "source_format": "xlsx",
  "sheet_name": "Sales",
  "table_id": "medium_Sales_t1",
  "row_start": 3, "row_end": 7,
  "source_ref": {"sheet_name": "Sales", "range_a1": "A3:E7",
                 "row_start": 3, "row_end": 7, "col_start": 1, "col_end": 5},
  "header_context": ["region", "q1_plan", "q1_fact", "q2_plan", "q2_fact"],
  "columns": [{"...": "ColumnSchema[]"}],
  "records": [{"region": "North", "q1_plan": "100", "...": "..."}],
  "text_projection": "Sheet: Sales\nTable: medium_Sales_t1\n..."
}
```

`row_start` / `row_end` and `range_a1` are **absolute sheet coordinates** —
they point straight to the cells you would highlight in Excel.

### `profile.json`

```jsonc
{
  "schema_version": "v1",
  "source_file": "examples/medium.xlsx",
  "source_format": "xlsx",
  "tables": [
    {
      "sheet_name": "Sales",
      "table_id": "medium_Sales_t1",
      "source_ref": { "...": "..." },
      "profile": {
        "row_count": 5, "column_count": 5,
        "empty_row_pct": 0.0,
        "warnings": ["possible_totals_row", "hidden_columns_present"],
        "columns": [
          {"name": "region", "inferred_type": "string", "missing_pct": 0.0,
           "distinct_count": 5,
           "top_values": [{"value": "North", "count": 1}]},
          {"name": "q1_plan", "inferred_type": "number", "missing_pct": 0.0,
           "numeric_min": 80.0, "numeric_max": 395.0, "numeric_avg": 159.0}
        ]
      }
    }
  ]
}
```

## Architecture

```
src/
├── models.py                 # Pydantic models (NormalizedDocument, ChunkModel, …)
├── load/
│   ├── encoding_detection.py # CSV encoding & separator sniffing
│   ├── header_detection.py   # multi-row header detection & name normalization
│   ├── type_inference.py     # column types + units extraction
│   ├── regions.py            # shared region builder (CSV + XLSX)
│   ├── csv_parser.py
│   ├── xlsx_parser.py
│   └── table_parser.py       # public API + streaming JSON writers
└── chunking/
    ├── table_chunker.py      # row-range chunking with absolute sheet coords
    └── text_projection.py    # text projection per chunk
```

Public API (in `src/load/table_parser.py`):
- `parse_table_file(path) -> NormalizedDocument`
- `export_artifacts(input_file, output_dir, settings) -> dict[str, str]`

Chunking settings (`ChunkSettings`):
- `max_rows_per_chunk` (default 200)
- `max_cells_per_chunk` (default 4000) — protects against very wide tables
- `preview_rows_in_text_projection` (default 20)

## Type inference rules

Inference runs over raw string values and never alters `rows`. Defaults are
intentionally conservative:

- 95% of non-empty values must match a type for that type to win.
- `0/1` columns are **number**, not bool. Bool requires actual literals
  (`true/false`, `yes/no`, `да/нет`) **and** both polarities present.
- Dates need a real date marker (separator or month name); pure short digits
  are never classified as dates.
- Locale-aware: `1,5` parses as `1.5` for numeric stats.

## Limits & known caveats

- "Vertical" (key-value) tables are detected heuristically — works well for
  short label/value sheets, may misfire for hand-crafted layouts.
- Multi-region split is by fully empty rows. Side-by-side tables on the same
  sheet are not split (would require a layout analyzer).
- `*.xls` (legacy) is not supported — use `.xlsx`.
