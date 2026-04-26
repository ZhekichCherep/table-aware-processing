from src.chunking.text_projection import build_text_projection


def test_collapses_consecutive_duplicates():
    proj = build_text_projection(
        sheet_name="S",
        table_id="t1",
        source_ref_a1="A1:E1",
        header_context=["col_1", "col_2", "col_3", "col_4", "col_5"],
        row_start=1,
        row_end=1,
        rows=[{"col_1": "X", "col_2": "X", "col_3": "X", "col_4": "Y", "col_5": "Z"}],
    )
    # X appears 3× consecutively → one entry with span marker.
    assert "X (×3)" in proj
    # Y and Z are singletons — no marker.
    assert "| Y |" in proj or "| Y" in proj
    assert "Z" in proj
    # No raw repetition of "X | X | X".
    assert "X | X | X" not in proj


def test_preserves_runs_separated_by_other_values():
    """Non-consecutive duplicates stay as separate entries (RLE preserves order)."""
    proj = build_text_projection(
        sheet_name="S",
        table_id="t1",
        source_ref_a1="A1:E1",
        header_context=["col_1", "col_2", "col_3", "col_4", "col_5"],
        row_start=1,
        row_end=1,
        rows=[{"col_1": "X", "col_2": "X", "col_3": "Y", "col_4": "X", "col_5": "X"}],
    )
    # Two separate runs of X (×2), not collapsed into one (×4).
    assert proj.count("X (×2)") == 2


def test_named_headers_keep_label():
    proj = build_text_projection(
        sheet_name="S",
        table_id="t1",
        source_ref_a1="A1:C1",
        header_context=["region", "region", "region"],
        row_start=1,
        row_end=1,
        rows=[{"region": "North"}],
    )
    # Only one item in this row (1 unique key + 1 unique value), label retained.
    assert "region: North" in proj


def test_truncation_message_after_dedup():
    """When items still exceed the cap after dedup, the message is informative."""
    rows = [{f"col_{i+1}": f"v{i}" for i in range(50)}]  # 50 unique values
    proj = build_text_projection(
        sheet_name="S",
        table_id="t1",
        source_ref_a1="A1:AX1",
        header_context=[f"col_{i+1}" for i in range(50)],
        row_start=1,
        row_end=1,
        rows=rows,
    )
    assert "more values" in proj
    # 50 unique values, cap is 12 → 38 omitted.
    assert "38" in proj
