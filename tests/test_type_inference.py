from src.load.type_inference import (
    date_stats,
    infer_column_type,
    numeric_stats,
    split_name_and_units,
)
from src.models import ColumnType


def test_empty_column():
    assert infer_column_type([]) == ColumnType.empty
    assert infer_column_type(["", " ", None]) == ColumnType.empty


def test_pure_numbers():
    assert infer_column_type(["1", "2", "3", "100", "0.5"]) == ColumnType.number


def test_numbers_tolerate_dirty_rows():
    # 95% rule: one bad value out of 20 must still be number.
    values = ["1"] * 19 + ["N/A"]
    assert infer_column_type(values) == ColumnType.number


def test_zero_one_is_not_bool():
    # The single most common misclassification: 0/1 flag column.
    assert infer_column_type(["0", "1", "1", "0", "1"]) == ColumnType.number


def test_bool_requires_actual_literals():
    assert infer_column_type(["true", "false", "true", "false"]) == ColumnType.bool
    assert infer_column_type(["yes", "no", "yes", "no"]) == ColumnType.bool
    assert infer_column_type(["да", "нет", "да", "нет"]) == ColumnType.bool


def test_bool_requires_both_polarities():
    # Constant "yes" — should not be bool (no information).
    assert infer_column_type(["yes"] * 10) != ColumnType.bool


def test_short_digit_strings_arent_dates():
    # Regression: pd.to_datetime(format="mixed") used to parse "7" as a date.
    assert infer_column_type(["7", "8", "9", "10"]) == ColumnType.number


def test_iso_date():
    assert infer_column_type(["2024-01-01", "2024-02-15", "2024-03-30"]) == ColumnType.date


def test_iso_datetime():
    values = ["2024-01-01 12:30:00", "2024-02-15 09:00:15"]
    assert infer_column_type(values) == ColumnType.datetime


def test_mixed_column():
    values = ["1", "2", "abc", "def", "3"]
    assert infer_column_type(values) == ColumnType.mixed


def test_numeric_stats_basic():
    lo, hi, avg = numeric_stats(["1", "2", "3", "4", "5"])
    assert lo == 1.0 and hi == 5.0 and avg == 3.0


def test_numeric_stats_locale_comma():
    lo, hi, _avg = numeric_stats(["1,5", "2,5", "3,5"])
    assert lo == 1.5 and hi == 3.5


def test_date_stats_iso_range():
    lo, hi = date_stats(["2024-01-01", "2024-06-15", "2024-12-31"])
    assert lo and hi
    assert lo.startswith("2024-01-01")
    assert hi.startswith("2024-12-31")


def test_split_units():
    assert split_name_and_units("Цена, руб.") == ("Цена", "руб")
    assert split_name_and_units("Weight (kg)") == ("Weight", "kg")
    # Should NOT strip a normal trailing word.
    _name, units = split_name_and_units("Total Sales")
    assert units is None
