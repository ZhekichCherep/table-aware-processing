from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from src.load.csv_parser import _build_table_region
from src.load.models import NormalizedDocument, SheetModel


def parse_xlsx(file_path: str | Path, *, top_n: int = 5) -> NormalizedDocument:
    path = Path(file_path)
    workbook = load_workbook(path, data_only=True)
    sheets: list[SheetModel] = []

    for ws in workbook.worksheets:
        frame = pd.read_excel(path, sheet_name=ws.title, dtype=str).fillna("")
        if frame.empty and frame.columns.size == 0:
            continue

        table = _build_table_region(
            frame=frame,
            sheet_name=ws.title,
            table_id=f"{path.stem}_{ws.title}_t1",
            top_n=top_n,
        )
        max_row = max(ws.max_row, 1)
        max_col = max(ws.max_column, 1)
        table.source_ref.range_a1 = f"A1:{get_column_letter(max_col)}{max_row}"
        table.source_ref.row_start = 1
        table.source_ref.row_end = max_row
        table.source_ref.col_start = 1
        table.source_ref.col_end = max_col
        sheets.append(SheetModel(sheet_name=ws.title, table_regions=[table]))

    return NormalizedDocument(
        source_file=str(path),
        source_format="xlsx",
        processed_at=pd.Timestamp.utcnow().to_pydatetime(),
        sheets=sheets,
    )
