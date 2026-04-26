from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from src.load.csv_parser import _build_table_regions
from src.load.models import (
    ColumnProfile,
    ColumnSchema,
    ColumnType,
    NormalizedDocument,
    SheetModel,
    SourceRef,
    TableProfile,
    TableRegion,
)


def _build_cells_region(
    sheet_name: str,
    table_id_prefix: str,
    ws,
    *,
    exclude_row_end: int,
    exclude_col_end: int,
) -> TableRegion:
    cells: list[dict[str, object]] = []
    for row in ws.iter_rows():
        for cell in row:
            if cell.row <= exclude_row_end and cell.column <= exclude_col_end:
                continue
            v = cell.value
            if v is None:
                continue
            s = str(v).strip()
            if s == "":
                continue
            cells.append(
                {
                    "cell": f"{cell.coordinate}={s}",
                }
            )

    max_row = max(ws.max_row, 1)
    max_col = max(ws.max_column, 1)
    source_ref = SourceRef(
        sheet_name=sheet_name,
        range_a1=f"A1:{get_column_letter(max_col)}{max_row}",
        row_start=1,
        row_end=max_row,
        col_start=1,
        col_end=max_col,
    )
    columns = [ColumnSchema(index=0, name_raw="cell", name_normalized="cell", inferred_type=ColumnType.string, missing_pct=0.0)]
    profile = TableProfile(
        row_count=len(cells),
        column_count=len(columns),
        empty_row_pct=0.0,
        warnings=["raw_cells_dump"],
        columns=[
            ColumnProfile(name="cell", inferred_type=ColumnType.string, missing_pct=0.0),
        ],
    )
    return TableRegion(
        table_id=f"{table_id_prefix}__cells",
        sheet_name=sheet_name,
        orientation="cells",
        header_rows=1,
        row_count=len(cells),
        column_count=len(columns),
        source_ref=source_ref,
        columns=columns,
        rows=cells,
        profile=profile,
    )


def parse_xlsx(file_path: str | Path, *, top_n: int = 5) -> NormalizedDocument:
    path = Path(file_path)
    workbook = load_workbook(path, data_only=True)
    sheets: list[SheetModel] = []

    for ws in workbook.worksheets:
        frame = pd.read_excel(path, sheet_name=ws.title, dtype=str).fillna("")
        if frame.empty and frame.columns.size == 0:
            continue

        table_id_prefix = f"{path.stem}_{ws.title}_t1"
        regions = _build_table_regions(
            frame=frame,
            sheet_name=ws.title,
            table_id=table_id_prefix,
            top_n=top_n,
        )
        max_row = max(ws.max_row, 1)
        max_col = max(ws.max_column, 1)
        for table in regions:
            table.source_ref.range_a1 = f"A1:{get_column_letter(max_col)}{max_row}"
            table.source_ref.row_start = 1
            table.source_ref.row_end = max_row
            table.source_ref.col_start = 1
            table.source_ref.col_end = max_col

        # Add raw cells region for content outside the parsed table rectangle.
        # Pandas read_excel() uses the first row as header by default.
        exclude_row_end = int(frame.shape[0]) + 1
        exclude_col_end = int(frame.shape[1])
        regions.append(
            _build_cells_region(
                ws.title,
                table_id_prefix,
                ws,
                exclude_row_end=exclude_row_end,
                exclude_col_end=exclude_col_end,
            )
        )
        sheets.append(SheetModel(sheet_name=ws.title, table_regions=regions))

    return NormalizedDocument(
        source_file=str(path),
        source_format="xlsx",
        processed_at=pd.Timestamp.utcnow().to_pydatetime(),
        sheets=sheets,
    )
