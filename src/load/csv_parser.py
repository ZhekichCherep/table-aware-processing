from __future__ import annotations

from pathlib import Path

import pandas as pd
import re

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

def _detect_orientation(frame: pd.DataFrame) -> str:
    # Heuristic: "vertical table" where first column stores field names and
    # subsequent columns store records (often few rows but many columns).
    if frame.empty or frame.shape[1] < 2:
        return "horizontal"
    rows, cols = frame.shape
    if rows < 2:
        return "horizontal"

    # Common signal: wide & short tables (transposed).
    if cols >= 8 and cols > rows * 1.5 and rows <= 30:
        return "vertical"

    # Key/value-like: first column mostly non-empty unique labels, and other columns carry values.
    first_col = frame.iloc[:, 0].astype(str).str.strip()
    non_empty = first_col[first_col != ""]
    if len(non_empty) >= max(3, int(rows * 0.7)):
        unique_ratio = non_empty.nunique() / max(1, len(non_empty))
        if unique_ratio >= 0.9 and cols <= 10:
            return "vertical"

    return "horizontal"


def _vertical_to_horizontal(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """
    Convert a vertical table:
      col0 = field name, col1.. = record values
    into a horizontal table:
      columns = field names, rows = records
    Returns (converted_frame, leftovers_rows).
    """
    if frame.empty or frame.shape[1] < 2:
        return frame, []

    keys = frame.iloc[:, 0].astype(str).str.strip().tolist()
    values = frame.iloc[:, 1:].copy()
    values = values.fillna("").astype(str)

    leftovers: list[dict[str, object]] = []
    good_key_mask = [k != "" for k in keys]
    if not all(good_key_mask):
        for ridx, (k, row) in enumerate(zip(keys, values.itertuples(index=False), strict=False), start=1):
            if k != "":
                continue
            row_dict = {f"col_{i+1}": str(v) for i, v in enumerate(row)}
            if any(str(v).strip() != "" for v in row_dict.values()):
                leftovers.append({"row_index": ridx, "cells": row_dict})

    keys_nonempty = [k if k != "" else f"field_{i+1}" for i, k in enumerate(keys)]
    values.index = keys_nonempty
    converted = values.T.reset_index(drop=True)
    converted.columns = keys_nonempty
    return converted, leftovers


_UNNAMED_RE = re.compile(r"^Unnamed:\s*\d+$", re.IGNORECASE)


def _normalize_column_name(name: object, idx: int) -> str:
    s = str(name).strip()
    if s == "" or _UNNAMED_RE.match(s):
        return f"col_{idx+1}"
    return s


def parse_csv(
    file_path: str | Path,
    *,
    encoding: str = "utf-8",
    top_n: int = 5,
) -> NormalizedDocument:
    path = Path(file_path)
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=encoding)
    frame = frame.fillna("")
    regions = _build_table_regions(frame, sheet_name="csv_sheet", table_id=f"{path.stem}_t1", top_n=top_n)
    return NormalizedDocument(
        source_file=str(path),
        source_format="csv",
        processed_at=pd.Timestamp.utcnow().to_pydatetime(),
        sheets=[SheetModel(sheet_name="csv_sheet", table_regions=regions)],
    )


def _build_table_regions(frame: pd.DataFrame, sheet_name: str, table_id: str, top_n: int) -> list[TableRegion]:
    orientation = _detect_orientation(frame)
    leftovers_rows: list[dict[str, object]] = []
    if orientation == "vertical":
        frame, leftovers_rows = _vertical_to_horizontal(frame)

    normalized_columns = [_normalize_column_name(c, idx) for idx, c in enumerate(frame.columns)]
    frame.columns = normalized_columns

    # Split table regions by fully empty rows.
    def _row_is_empty(r: pd.Series) -> bool:
        return r.astype(str).str.strip().eq("").all()

    empty_mask = frame.apply(_row_is_empty, axis=1) if not frame.empty else pd.Series([], dtype=bool)
    regions: list[TableRegion] = []
    start = 0
    part_idx = 1
    n = len(frame)
    while start < n:
        while start < n and bool(empty_mask.iloc[start]):
            start += 1
        if start >= n:
            break
        end = start
        while end < n and not bool(empty_mask.iloc[end]):
            end += 1
        part = frame.iloc[start:end].copy()
        if not part.empty:
            column_schemas: list[ColumnSchema] = []
            column_profiles: list[ColumnProfile] = []
            for idx, col in enumerate(part.columns):
                series = part[col]
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

            warnings = _build_warnings(part, column_schemas)
            source_ref = SourceRef(
                sheet_name=sheet_name,
                range_a1=None,
                row_start=1,
                row_end=max(1, len(frame) + 1),
                col_start=1,
                col_end=max(1, len(part.columns)),
            )
            profile = TableProfile(
                row_count=len(part),
                column_count=len(part.columns),
                empty_row_pct=_empty_row_pct(part),
                warnings=warnings,
                columns=column_profiles,
            )
            rows = part.to_dict(orient="records")
            region_id = table_id if part_idx == 1 else f"{table_id}__part{part_idx}"
            regions.append(
                TableRegion(
                    table_id=region_id,
                    sheet_name=sheet_name,
                    orientation=orientation,
                    header_rows=1,
                    row_count=len(part),
                    column_count=len(part.columns),
                    source_ref=source_ref,
                    columns=column_schemas,
                    rows=rows,
                    profile=profile,
                )
            )
            part_idx += 1
        start = end

    if not regions:
        # Fallback: keep at least one empty region.
        source_ref = SourceRef(
            sheet_name=sheet_name,
            range_a1=None,
            row_start=1,
            row_end=1,
            col_start=1,
            col_end=max(1, len(frame.columns)),
        )
        profile = TableProfile(row_count=0, column_count=len(frame.columns), empty_row_pct=0.0, warnings=[], columns=[])
        regions = [
            TableRegion(
                table_id=table_id,
                sheet_name=sheet_name,
                orientation=orientation,
                header_rows=1,
                row_count=0,
                column_count=len(frame.columns),
                source_ref=source_ref,
                columns=[],
                rows=[],
                profile=profile,
            )
        ]

    main_region = regions[0]
    if leftovers_rows:
        leftovers_columns = [
            ColumnSchema(
                index=0,
                name_raw="row_index",
                name_normalized="row_index",
                inferred_type=ColumnType.number,
                missing_pct=0.0,
            ),
            ColumnSchema(
                index=1,
                name_raw="cells",
                name_normalized="cells",
                inferred_type=ColumnType.mixed,
                missing_pct=0.0,
            ),
        ]
        leftovers_profile = TableProfile(
            row_count=len(leftovers_rows),
            column_count=2,
            empty_row_pct=0.0,
            warnings=["leftovers_from_vertical_orientation"],
            columns=[
                ColumnProfile(name="row_index", inferred_type=ColumnType.number, missing_pct=0.0),
                ColumnProfile(name="cells", inferred_type=ColumnType.mixed, missing_pct=0.0),
            ],
        )
        leftovers_region = TableRegion(
            table_id=f"{table_id}__leftovers",
            sheet_name=sheet_name,
            orientation="leftovers",
            header_rows=1,
            row_count=len(leftovers_rows),
            column_count=2,
            source_ref=main_region.source_ref,
            columns=leftovers_columns,
            rows=leftovers_rows,
            profile=leftovers_profile,
        )
        main_region.profile.warnings.append("has_leftovers_region")
        regions.append(leftovers_region)

    return regions


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
