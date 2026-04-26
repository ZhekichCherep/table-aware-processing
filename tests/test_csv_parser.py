from pathlib import Path

from src.load.csv_parser import parse_csv


def test_parse_csv_basic(tmp_path: Path):
    p = tmp_path / "small.csv"
    p.write_text("id,name,salary\n1,Alice,100\n2,Bob,200\n", encoding="utf-8")
    doc = parse_csv(p)

    assert doc.source_format == "csv"
    assert len(doc.sheets) == 1
    sheet = doc.sheets[0]
    assert len(sheet.table_regions) == 1
    region = sheet.table_regions[0]
    assert region.header_rows == 1
    assert [c.name_normalized for c in region.columns] == ["id", "name", "salary"]
    assert region.row_count == 2
    # Sheet coordinates: header at row 1, data at rows 2..3.
    assert region.source_ref.row_start == 1
    assert region.source_ref.row_end == 3


def test_parse_csv_semicolon_separator(tmp_path: Path):
    p = tmp_path / "ru.csv"
    p.write_text("id;name;salary\n1;Alice;100\n2;Bob;200\n", encoding="utf-8")
    doc = parse_csv(p)
    region = doc.sheets[0].table_regions[0]
    assert region.row_count == 2
    assert [c.name_normalized for c in region.columns] == ["id", "name", "salary"]


def test_parse_csv_cp1251(tmp_path: Path):
    p = tmp_path / "cp1251.csv"
    text = "Имя,Зарплата\nАлиса,100\nБоб,200\n"
    p.write_bytes(text.encode("cp1251"))
    doc = parse_csv(p)
    region = doc.sheets[0].table_regions[0]
    # Header should round-trip even from cp1251.
    raw = [c.name_raw for c in region.columns]
    assert "Имя" in raw and "Зарплата" in raw


def test_parse_csv_empty_row_split(tmp_path: Path):
    p = tmp_path / "split.csv"
    p.write_text("id,name\n1,a\n2,b\n\n3,c\n4,d\n", encoding="utf-8")
    doc = parse_csv(p)
    regions = doc.sheets[0].table_regions
    # Two regions split by an empty row.
    assert len(regions) == 2
    # Coordinates of secondary region must reflect the actual sheet position.
    assert regions[1].source_ref.row_start > regions[0].source_ref.row_end


def test_parse_csv_units_in_header(tmp_path: Path):
    p = tmp_path / "units.csv"
    p.write_text("id,price (RUB),weight (kg)\n1,100,5\n2,200,10\n", encoding="utf-8")
    doc = parse_csv(p)
    region = doc.sheets[0].table_regions[0]
    units = {c.name_normalized: c.units for c in region.columns}
    assert units["price"] == "RUB"
    assert units["weight"] == "kg"
