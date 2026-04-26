from src.load.header_detection import build_column_names, detect_header_rows


def test_single_row_header():
    rows = [
        ["id", "name", "salary"],
        [1, "Alice", 100],
        [2, "Bob", 200],
    ]
    assert detect_header_rows(rows) == 1


def test_two_row_header():
    rows = [
        ["", "Q1", "Q1", "Q2", "Q2"],
        ["Region", "Plan", "Fact", "Plan", "Fact"],
        ["North", 100, 92, 110, 118],
        ["South", 80, 88, 90, 75],
    ]
    assert detect_header_rows(rows) == 2


def test_header_stops_at_data_row():
    rows = [
        ["a", "b", "c"],
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
    ]
    assert detect_header_rows(rows) == 1


def test_no_data_at_all():
    # Header-only sheet — return at least 1.
    assert detect_header_rows([["a", "b"]]) == 1


def test_build_column_names_joins_two_rows():
    header = [
        ["", "Q1", "Q1", "Q2"],
        ["Region", "Plan", "Fact", "Plan"],
    ]
    pairs = build_column_names(header, 4)
    raw = [r for r, _ in pairs]
    norm = [n for _, n in pairs]
    assert raw[0] == "Region"
    assert raw[1] == "Q1 / Plan"
    assert raw[2] == "Q1 / Fact"
    assert raw[3] == "Q2 / Plan"
    assert norm[0] == "region"
    assert norm[1] == "q1_plan"


def test_build_column_names_dedupes():
    header = [["name", "name", "name"]]
    pairs = build_column_names(header, 3)
    norm = [n for _, n in pairs]
    assert len(set(norm)) == 3


def test_build_column_names_for_unnamed_columns():
    header = [["Unnamed: 0", "value", ""]]
    pairs = build_column_names(header, 3)
    raw = [r for r, _ in pairs]
    norm = [n for _, n in pairs]
    assert norm[0] == "col_1"
    assert norm[1] == "value"
    assert norm[2] == "col_3"
    # Raw should fall back to normalized name when truly empty.
    assert raw[0] == "col_1"
