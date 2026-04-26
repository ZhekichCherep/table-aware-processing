"""Tests for the shared region builder."""

from src.load.regions import build_table_regions


def _names(region):
    return [c.name_normalized for c in region.columns]


def test_secondary_region_has_no_header_rows():
    rows = [
        {"a": "1", "b": "x"},
        {"a": "2", "b": "y"},
        {"a": "", "b": ""},
        {"a": "3", "b": "z"},
    ]
    regions = build_table_regions(
        sheet_name="S",
        table_id_prefix="t",
        column_names_raw=["a", "b"],
        column_names_normalized=["a", "b"],
        column_units=[None, None],
        rows=rows,
        header_rows=1,
        row_offset=2,  # data rows start at sheet row 2
        col_start=1,
        col_end=2,
        range_a1_func=lambda *_: None,
    )
    assert len(regions) == 2
    # Primary region: includes header row in its bounding box.
    assert regions[0].header_rows == 1
    assert regions[0].source_ref.row_start == 1
    assert regions[0].source_ref.row_end == 3
    # Secondary region: data only, no header rows.
    assert regions[1].header_rows == 0
    assert regions[1].source_ref.row_start == 5
    assert regions[1].source_ref.row_end == 5


def test_totals_row_warning():
    rows = [
        {"name": "a", "amt": "10"},
        {"name": "b", "amt": "20"},
        {"name": "Итого", "amt": "30"},
    ]
    regions = build_table_regions(
        sheet_name="S",
        table_id_prefix="t",
        column_names_raw=["name", "amt"],
        column_names_normalized=["name", "amt"],
        column_units=[None, None],
        rows=rows,
        header_rows=1,
        row_offset=2,
        col_start=1,
        col_end=2,
        range_a1_func=lambda *_: None,
    )
    assert "possible_totals_row" in regions[0].profile.warnings
