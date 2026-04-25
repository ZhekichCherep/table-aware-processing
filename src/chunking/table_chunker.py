from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.load.models import ChunkModel, NormalizedDocument, SourceRef, TableRegion
from src.chunking.text_projection import build_text_projection


@dataclass(frozen=True)
class ChunkSettings:
    max_rows_per_chunk: int = 200
    max_cells_per_chunk: int = 4000


def build_chunks(
    doc: NormalizedDocument,
    settings: ChunkSettings | None = None,
) -> list[ChunkModel]:
    cfg = settings or ChunkSettings()
    chunks: list[ChunkModel] = []
    for sheet in doc.sheets:
        for table in sheet.table_regions:
            chunks.extend(_chunk_table(doc, table, cfg))
    return chunks


def _chunk_table(
    doc: NormalizedDocument,
    table: TableRegion,
    settings: ChunkSettings,
) -> list[ChunkModel]:
    rows = table.rows
    columns_count = max(1, len(table.columns))
    max_by_cells = max(1, settings.max_cells_per_chunk // columns_count)
    rows_per_chunk = max(1, min(settings.max_rows_per_chunk, max_by_cells))
    out: list[ChunkModel] = []
    header_context = [col.name_normalized for col in table.columns]

    for i in range(0, len(rows), rows_per_chunk):
        part = rows[i : i + rows_per_chunk]
        row_start = i + 1
        row_end = i + len(part)
        source_ref = SourceRef(
            sheet_name=table.sheet_name,
            range_a1=table.source_ref.range_a1,
            row_start=row_start,
            row_end=row_end,
            col_start=table.source_ref.col_start,
            col_end=table.source_ref.col_end,
        )
        out.append(
            ChunkModel(
                chunk_id=f"{Path(doc.source_file).stem}:{table.table_id}:{row_start}-{row_end}",
                source_file=doc.source_file,
                source_format=doc.source_format,
                sheet_name=table.sheet_name,
                table_id=table.table_id,
                row_start=row_start,
                row_end=row_end,
                source_ref=source_ref,
                header_context=header_context,
                columns=table.columns,
                records=part,
                text_projection=build_text_projection(
                    table.sheet_name,
                    table.table_id,
                    source_ref.range_a1,
                    header_context,
                    row_start,
                    row_end,
                    part,
                ),
            )
        )
    return out
