from datetime import datetime, timezone

from src.chunking.table_chunker import ChunkSettings, build_chunks
from src.models import (
    ColumnSchema,
    ColumnType,
    NormalizedDocument,
    SheetModel,
    SourceRef,
    TableProfile,
    TableRegion,
)


def _make_doc(*, header_rows: int, row_offset: int, n_rows: int) -> NormalizedDocument:
    columns = [
        ColumnSchema(index=0, name_raw="id", name_normalized="id",
                     inferred_type=ColumnType.number, missing_pct=0.0),
        ColumnSchema(index=1, name_raw="name", name_normalized="name",
                     inferred_type=ColumnType.string, missing_pct=0.0),
    ]
    rows = [{"id": str(i + 1), "name": f"row{i+1}"} for i in range(n_rows)]
    region_row_start = max(1, row_offset - header_rows)
    region_row_end = row_offset + n_rows - 1
    region = TableRegion(
        table_id="t1",
        sheet_name="S",
        orientation="horizontal",
        header_rows=header_rows,
        row_count=n_rows,
        column_count=2,
        source_ref=SourceRef(
            sheet_name="S",
            range_a1=f"A{region_row_start}:B{region_row_end}",
            row_start=region_row_start,
            row_end=region_row_end,
            col_start=1,
            col_end=2,
        ),
        columns=columns,
        rows=rows,
        profile=TableProfile(
            row_count=n_rows, column_count=2, empty_row_pct=0.0,
            warnings=[], columns=[],
        ),
    )
    return NormalizedDocument(
        source_file="test.xlsx",
        source_format="xlsx",
        processed_at=datetime.now(timezone.utc),
        sheets=[SheetModel(sheet_name="S", table_regions=[region])],
    )


def test_chunk_row_coordinates_match_sheet_rows():
    """Chunks must reference real sheet row numbers, not 0-based slice indices."""
    doc = _make_doc(header_rows=2, row_offset=3, n_rows=10)
    chunks = build_chunks(doc, ChunkSettings(max_rows_per_chunk=4, max_cells_per_chunk=10000))
    assert len(chunks) == 3

    # First chunk: data rows 3..6 in the sheet.
    assert chunks[0].row_start == 3
    assert chunks[0].row_end == 6
    assert chunks[0].source_ref.row_start == 3
    assert chunks[0].source_ref.row_end == 6
    assert chunks[0].source_ref.range_a1 == "A3:B6"

    # Last chunk: data rows 11..12 (10 rows of data starting at 3).
    assert chunks[-1].row_start == 11
    assert chunks[-1].row_end == 12


def test_chunk_id_is_unique_and_includes_coords():
    doc = _make_doc(header_rows=1, row_offset=2, n_rows=5)
    chunks = build_chunks(doc, ChunkSettings(max_rows_per_chunk=2, max_cells_per_chunk=10000))
    ids = [c.chunk_id for c in chunks]
    assert len(set(ids)) == len(ids)
    assert ids[0].endswith(":2-3")


def test_max_cells_caps_rows_per_chunk():
    doc = _make_doc(header_rows=1, row_offset=2, n_rows=20)
    # 2 columns, max_cells=4 -> 2 rows per chunk.
    chunks = build_chunks(doc, ChunkSettings(max_rows_per_chunk=1000, max_cells_per_chunk=4))
    assert all(len(c.records) <= 2 for c in chunks)
    assert len(chunks) == 10


def test_header_context_inherited_in_every_chunk():
    doc = _make_doc(header_rows=1, row_offset=2, n_rows=5)
    chunks = build_chunks(doc, ChunkSettings(max_rows_per_chunk=2, max_cells_per_chunk=10000))
    for c in chunks:
        assert c.header_context == ["id", "name"]
