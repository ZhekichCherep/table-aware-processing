# table-aware-processing

Table-aware processing module for `.xlsx` and `.csv` files with:
- structural extraction (sheet/table/columns/source refs),
- column typing and profiling,
- table-aware chunking for RAG.

## Requirements

- Python 3.10+
- `pandas`
- `openpyxl`
- `xlrd`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run Demo

Process all `.xlsx` and `.csv` files from `test_data`:

```bash
python run_demo.py
```

Artifacts are saved to `output/`.

## Output Contract (schema v1)

For each input file parser produces:
- `*.normalized.json` - normalized structured representation.
- `*.chunks.jsonl` - RAG-ready chunks by row ranges.
- `*.profile.json` - profiling and warnings.

### normalized.json

Top-level object: `NormalizedDocument`
- `schema_version`
- `source_file`
- `source_format`
- `processed_at`
- `sheets[]`
  - `sheet_name`
  - `table_regions[]`
    - `table_id`
    - `sheet_name`
    - `header_rows`
    - `row_count`, `column_count`
    - `source_ref` (`sheet_name`, `range_a1`, `row_start`, `row_end`, `col_start`, `col_end`)
    - `columns[]` (`index`, `name_raw`, `name_normalized`, `inferred_type`, `missing_pct`)
    - `rows[]` (raw values, no loss)
    - `profile`

### chunks.jsonl

Each line is one `ChunkModel`:
- `chunk_id`
- `source_file`, `source_format`
- `sheet_name`, `table_id`
- `row_start`, `row_end`
- `source_ref`
- `header_context`
- `columns`
- `records`
- `text_projection`

`text_projection` format:

```text
Sheet: <name>
Table: <id>
Columns: c1, c2, c3
Rows <start>-<end>:
<preview rows>
```

### profile.json

- `schema_version`
- `source_file`
- `source_format`
- `tables[]`
  - `sheet_name`
  - `table_id`
  - `source_ref`
  - `profile`
    - table metrics: `row_count`, `column_count`, `empty_row_pct`, `warnings[]`
    - per-column metrics: `missing_pct`, `inferred_type`, numeric stats (`min/max/avg`) and top values.

Warnings currently include:
- `many_empty_rows`
- `mixed_types_in_column:<column_name>`
- `possible_totals_row`

## Main API

In `src/load/table_parser.py`:
- `parse_table_file(path)` -> `NormalizedDocument`
- `export_artifacts(input_file, output_dir, settings)` -> output file paths

Chunking settings:
- `max_rows_per_chunk`
- `max_cells_per_chunk`

Chunking implementation is located in `src/chunking/table_chunker.py`:
- `build_chunks(doc, settings)` -> `list[ChunkModel]`

Text projection builder is located in `src/chunking/text_projection.py`:
- `build_text_projection(...)` -> `str`
