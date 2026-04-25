from __future__ import annotations

from pathlib import Path

import pandas as pd

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


def parse_csv(
    file_path: str | Path,
    *,
    encoding: str = "utf-8",
    top_n: int = 5,
) -> NormalizedDocument:
    path = Path(file_path)
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=encoding)
    frame = frame.fillna("")
    table = _build_table_region(frame, sheet_name="csv_sheet", table_id=f"{path.stem}_t1", top_n=top_n)
    return NormalizedDocument(
        source_file=str(path),
        source_format="csv",
        processed_at=pd.Timestamp.utcnow().to_pydatetime(),
        sheets=[SheetModel(sheet_name="csv_sheet", table_regions=[table])],
    )


def _build_table_region(frame: pd.DataFrame, sheet_name: str, table_id: str, top_n: int) -> TableRegion:
    normalized_columns = [str(c).strip() or f"col_{idx+1}" for idx, c in enumerate(frame.columns)]
    frame.columns = normalized_columns
    column_schemas: list[ColumnSchema] = []
    column_profiles: list[ColumnProfile] = []

    for idx, col in enumerate(frame.columns):
        series = frame[col]
        inferred = _infer_column_type(series)
        missing_pct = float((series.eq("").sum() / len(series) * 100.0) if len(series) else 0.0)
        column_schemas.append(
            ColumnSchema(
                index=idx,
                name_raw=col,
                name_normalized=col,
                inferred_type=inferred,
                missing_pct=missing_pct,
            )
        )
        column_profiles.append(_profile_column(col, series, inferred, missing_pct, top_n))

    warnings = _build_warnings(frame, column_schemas)
    source_ref = SourceRef(
        sheet_name=sheet_name,
        range_a1=None,
        row_start=1,
        row_end=max(1, len(frame) + 1),
        col_start=1,
        col_end=max(1, len(frame.columns)),
    )
    profile = TableProfile(
        row_count=len(frame),
        column_count=len(frame.columns),
        empty_row_pct=_empty_row_pct(frame),
        warnings=warnings,
        columns=column_profiles,
    )
    rows = frame.to_dict(orient="records")
    return TableRegion(
        table_id=table_id,
        sheet_name=sheet_name,
        header_rows=1,
        row_count=len(frame),
        column_count=len(frame.columns),
        source_ref=source_ref,
        columns=column_schemas,
        rows=rows,
        profile=profile,
    )


def _infer_column_type(series: pd.Series) -> ColumnType:
    non_empty = series[series.astype(str).str.strip() != ""]
    if non_empty.empty:
        return ColumnType.empty

    lowered = non_empty.astype(str).str.strip().str.lower()
    if lowered.isin({"true", "false", "yes", "no", "0", "1"}).all():
        return ColumnType.bool

    numeric = pd.to_numeric(non_empty, errors="coerce")
    if numeric.notna().all():
        return ColumnType.number

    dt = pd.to_datetime(non_empty, errors="coerce", format="mixed")
    if dt.notna().all():
        if any((d.hour or d.minute or d.second) for d in dt.dropna()):
            return ColumnType.datetime
        return ColumnType.date

    if numeric.notna().any() or dt.notna().any():
        return ColumnType.mixed
    return ColumnType.string


def _profile_column(
    name: str, series: pd.Series, inferred_type: ColumnType, missing_pct: float, top_n: int
) -> ColumnProfile:
    non_empty = series[series.astype(str).str.strip() != ""]
    profile = ColumnProfile(name=name, inferred_type=inferred_type, missing_pct=missing_pct)

    if inferred_type == ColumnType.number:
        numeric = pd.to_numeric(non_empty, errors="coerce").dropna()
        if not numeric.empty:
            profile.numeric_min = float(numeric.min())
            profile.numeric_max = float(numeric.max())
            profile.numeric_avg = float(numeric.mean())

    if inferred_type in {ColumnType.string, ColumnType.mixed, ColumnType.bool}:
        counts = non_empty.astype(str).value_counts().head(top_n)
        profile.top_values = [{"value": idx, "count": int(val)} for idx, val in counts.items()]

    return profile


def _empty_row_pct(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    empty_rows = frame.apply(lambda row: row.astype(str).str.strip().eq("").all(), axis=1).sum()
    return float(empty_rows / len(frame) * 100.0)


def _build_warnings(frame: pd.DataFrame, columns: list[ColumnSchema]) -> list[str]:
    warnings: list[str] = []
    if _empty_row_pct(frame) > 20:
        warnings.append("many_empty_rows")
    for col in columns:
        if col.inferred_type == ColumnType.mixed:
            warnings.append(f"mixed_types_in_column:{col.name_normalized}")
    # Scan tail rows where summary rows are usually located.
    if len(frame) >= 2:
        tail_size = min(5, len(frame))
        tail = frame.tail(tail_size).astype(str).apply(lambda col: col.str.lower())
        totals_pattern = r"\b(?:итог|итого|total|sum|subtotal)\b"
        if tail.apply(lambda col: col.str.contains(totals_pattern, regex=True)).any().any():
            warnings.append("possible_totals_row")
    return warnings
