"""Shared region builder used by both CSV and XLSX parsers.

A "region" is one logical table — a contiguous block of data rows under the
same header. A single sheet can produce multiple regions when the data is
broken up by fully empty rows.

This module is parser-agnostic: it consumes already-prepared rows and column
metadata together with a `row_offset` that tells it where row 0 of the data
sits in the original file (1-based sheet coordinate). All resulting regions
carry correct `source_ref` ranges back into the source file.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from src.load.type_inference import (
    date_stats,
    infer_column_type,
    numeric_stats,
)
from src.models import (
    ColumnProfile,
    ColumnSchema,
    ColumnType,
    SourceRef,
    TableProfile,
    TableRegion,
)

TOTALS_PATTERN = re.compile(
    r"(?:^|[^\w])(?:итог|итого|всего|total|sum|subtotal|grand\s+total)(?:[^\w]|$)",
    re.IGNORECASE,
)


def _row_is_empty(row: dict[str, Any]) -> bool:
    return all(v is None or str(v).strip() == "" for v in row.values())


def _empty_row_pct(rows: Sequence[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    n_empty = sum(1 for r in rows if _row_is_empty(r))
    return float(n_empty / len(rows) * 100.0)


def _profile_column(
    name: str,
    values: Sequence[object],
    inferred_type: ColumnType,
    missing_pct: float,
    *,
    top_n: int,
) -> ColumnProfile:
    non_empty = [v for v in values if v is not None and str(v).strip() != ""]
    profile = ColumnProfile(
        name=name,
        inferred_type=inferred_type,
        missing_pct=missing_pct,
        distinct_count=len({str(v).strip() for v in non_empty}) if non_empty else 0,
    )
    if inferred_type == ColumnType.number:
        lo, hi, avg = numeric_stats(non_empty)
        profile.numeric_min, profile.numeric_max, profile.numeric_avg = lo, hi, avg
    if inferred_type in (ColumnType.date, ColumnType.datetime):
        lo, hi = date_stats(non_empty)
        profile.date_min, profile.date_max = lo, hi
    if inferred_type in (ColumnType.string, ColumnType.mixed, ColumnType.bool):
        counts: dict[str, int] = {}
        for v in non_empty:
            s = str(v).strip()
            counts[s] = counts.get(s, 0) + 1
        ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
        profile.top_values = [{"value": k, "count": v} for k, v in ranked]
    return profile


def _build_table_warnings(
    rows: Sequence[dict[str, Any]],
    columns: Sequence[ColumnSchema],
    extra: Sequence[str] = (),
) -> list[str]:
    warnings: list[str] = list(extra)
    if _empty_row_pct(rows) > 20:
        warnings.append("many_empty_rows")
    for col in columns:
        if col.inferred_type == ColumnType.mixed:
            warnings.append(f"mixed_types_in_column:{col.name_normalized}")
    if len(rows) >= 2:
        tail = rows[-min(5, len(rows)):]
        for row in tail:
            for v in row.values():
                if v is None:
                    continue
                if TOTALS_PATTERN.search(str(v)):
                    warnings.append("possible_totals_row")
                    return warnings
    return warnings


def looks_like_vertical_grid(grid: Sequence[Sequence[str]]) -> bool:
    """Pre-check on the raw grid (before header extraction) to decide whether
    the table is a key/value layout. If it is, parsers should bypass header
    detection — every row is data, and the transposition happens later inside
    `build_table_regions`.
    """
    from src.load.type_inference import _try_parse_date, _try_parse_number

    if len(grid) < 2 or len(grid) > 30:
        return False
    width = max((len(r) for r in grid), default=0)
    if width < 2:
        return False

    first_col = [(grid[r][0] if len(grid[r]) > 0 else "") for r in range(len(grid))]
    non_empty = [str(s).strip() for s in first_col if str(s).strip() != ""]
    if len(non_empty) < max(3, int(len(grid) * 0.7)):
        return False
    if len(set(non_empty)) / len(non_empty) < 0.9:
        return False
    numeric_labels = sum(1 for s in non_empty if _try_parse_number(s))
    if numeric_labels > len(non_empty) // 2:
        return False

    if width == 2:
        # 2-col vertical (key/value records) needs more evidence than wider
        # cases — small horizontal CSVs (3 rows × 2 cols with a header row)
        # would otherwise be misread as transposed.
        if len(grid) < 4:
            return False
        values = [(grid[r][1] if len(grid[r]) > 1 else "") for r in range(len(grid))]
        non_empty_vals = [str(v).strip() for v in values if str(v).strip() != ""]
        if len(non_empty_vals) < max(3, int(len(grid) * 0.7)):
            return False
        type_kinds: set[str] = set()
        n_string = 0
        for v in non_empty_vals:
            if _try_parse_number(v):
                type_kinds.add("num")
            elif _try_parse_date(v)[0]:
                type_kinds.add("date")
            else:
                type_kinds.add("str")
                n_string += 1
        # Require >= 2 actual string values: a single string is almost always
        # a header word that got swept in with the data.
        return len(type_kinds) >= 2 and n_string >= 2

    return width >= 4 and width > len(grid) * 1.5


def _detect_orientation(rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> str:
    """Detect vertical (transposed) tables.

    Two main vertical shapes are recognized:
    - Wide-and-short (>=4 cols, transposed multi-record): col0 is unique
      descriptive labels, other columns carry per-record values.
    - 2-column key/value: col0 is descriptive labels, col1 is heterogeneous
      values (a mix of numbers / dates / strings).

    We're conservative about labels: they must be mostly *non-numeric* —
    otherwise plain "id | name" tables would get mistakenly transposed.
    """
    from src.load.type_inference import _try_parse_date, _try_parse_number

    if not rows or len(columns) < 2:
        return "horizontal"
    n_rows = len(rows)
    n_cols = len(columns)
    if n_rows < 2 or n_rows > 30:
        return "horizontal"

    first_col = columns[0]
    labels = [str(r.get(first_col, "")).strip() for r in rows]
    non_empty = [s for s in labels if s != ""]
    if len(non_empty) < max(3, int(n_rows * 0.7)):
        return "horizontal"

    unique_ratio = len(set(non_empty)) / max(1, len(non_empty))
    if unique_ratio < 0.9:
        return "horizontal"

    numeric_labels = sum(1 for s in non_empty if _try_parse_number(s))
    if numeric_labels > len(non_empty) // 2:
        return "horizontal"

    if n_cols >= 4 and n_cols > n_rows * 1.5:
        return "vertical"

    if n_cols == 2:
        value_col = columns[1]
        values = [str(r.get(value_col, "")).strip() for r in rows]
        non_empty_vals = [v for v in values if v != ""]
        if len(non_empty_vals) < max(3, int(n_rows * 0.7)):
            return "horizontal"
        type_kinds: set[str] = set()
        for v in non_empty_vals:
            if _try_parse_number(v):
                type_kinds.add("num")
            elif _try_parse_date(v)[0]:
                type_kinds.add("date")
            else:
                type_kinds.add("str")
        if len(type_kinds) >= 2:
            return "vertical"

    return "horizontal"


def _vertical_to_horizontal(
    rows: list[dict[str, Any]],
    columns: list[str],
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    """Transpose a vertical table.

    Vertical layout: column 0 = field name, columns 1..N = record values.
    Returns (new_rows, new_columns, leftovers).
    """
    if not rows or len(columns) < 2:
        return rows, columns, []

    label_col, value_cols = columns[0], columns[1:]
    field_names: list[str] = []
    leftovers: list[dict[str, Any]] = []

    for ridx, row in enumerate(rows, start=1):
        label = str(row.get(label_col, "")).strip()
        if label == "":
            row_dict = {f"col_{i + 1}": str(row.get(c, "") or "") for i, c in enumerate(value_cols)}
            if any(v.strip() != "" for v in row_dict.values()):
                leftovers.append({"row_index": ridx, "cells": row_dict})
            field_names.append(f"field_{ridx}")
        else:
            field_names.append(label)

    seen: dict[str, int] = {}
    unique_field_names: list[str] = []
    for f in field_names:
        if f in seen:
            seen[f] += 1
            unique_field_names.append(f"{f}_{seen[f]}")
        else:
            seen[f] = 1
            unique_field_names.append(f)

    new_rows: list[dict[str, Any]] = []
    for vc in value_cols:
        record = {f: row.get(vc, "") for f, row in zip(unique_field_names, rows, strict=True)}
        new_rows.append(record)
    return new_rows, unique_field_names, leftovers


def build_table_regions(
    *,
    sheet_name: str,
    table_id_prefix: str,
    column_names_raw: list[str],
    column_names_normalized: list[str],
    column_units: list[str | None],
    rows: list[dict[str, Any]],
    header_rows: int,
    row_offset: int,
    col_start: int,
    col_end: int,
    range_a1_func,
    extra_warnings: Sequence[str] = (),
    top_n: int = 5,
    orientation_hint: str | None = None,
) -> list[TableRegion]:
    """Split rows into one or more table regions, each with proper source_ref.

    Args:
        ... (see existing args)
        orientation_hint: When set to "vertical" by the parser (because it
            already pre-detected a key/value layout), the input `rows` represent
            *physical* rows on the sheet — they will be transposed into a single
            record and `source_ref` is anchored to the pre-transpose extent.

    Returns:
        A list of TableRegion objects. The first region contains the header
        rows in its `source_ref`; any sub-regions split off by empty rows have
        `header_rows = 0` and inherit columns from the primary region.
    """
    orientation = "horizontal"
    leftovers_rows: list[dict[str, Any]] = []

    # Parser-level vertical hint: input `rows` are physical sheet rows. Capture
    # their extent before transposing so source_ref still points to the
    # original block in the file.
    physical_row_count = len(rows) if orientation_hint == "vertical" else None

    if orientation_hint == "vertical" and rows and column_names_normalized:
        orientation = "vertical"
        new_rows, new_cols, leftovers_rows = _vertical_to_horizontal(rows, column_names_normalized)
        rows = new_rows
        column_names_raw = new_cols
        column_names_normalized = new_cols
        column_units = [None] * len(new_cols)
    elif rows and column_names_normalized:
        # Heuristic vertical detection on already-headered data.
        if _detect_orientation(rows, column_names_normalized) == "vertical":
            orientation = "vertical"
            physical_row_count = len(rows)
            new_rows, new_cols, leftovers_rows = _vertical_to_horizontal(rows, column_names_normalized)
            rows = new_rows
            column_names_raw = new_cols
            column_names_normalized = new_cols
            column_units = [None] * len(new_cols)

    # Empty-row split — produces a list of (start_idx, end_idx) inclusive ranges.
    spans: list[tuple[int, int]] = []
    n = len(rows)
    i = 0
    while i < n:
        while i < n and _row_is_empty(rows[i]):
            i += 1
        if i >= n:
            break
        j = i
        while j < n and not _row_is_empty(rows[j]):
            j += 1
        spans.append((i, j - 1))
        i = j

    regions: list[TableRegion] = []
    for part_idx, (s, e) in enumerate(spans, start=1):
        part_rows = rows[s : e + 1]
        column_schemas: list[ColumnSchema] = []
        column_profiles: list[ColumnProfile] = []
        for col_idx, (raw_name, norm_name, units) in enumerate(
            zip(column_names_raw, column_names_normalized, column_units, strict=True)
        ):
            values = [row.get(norm_name, "") for row in part_rows]
            inferred = infer_column_type(values)
            missing = sum(1 for v in values if v is None or str(v).strip() == "")
            missing_pct = float(missing / len(values) * 100.0) if values else 0.0
            column_schemas.append(
                ColumnSchema(
                    index=col_idx,
                    name_raw=raw_name,
                    name_normalized=norm_name,
                    inferred_type=inferred,
                    missing_pct=missing_pct,
                    units=units,
                )
            )
            column_profiles.append(
                _profile_column(norm_name, values, inferred, missing_pct, top_n=top_n)
            )

        # Region's sheet-coordinate bounding box.
        # Primary region: include the header rows above the data.
        # Subsequent regions: data only (header is inherited).
        # Vertical regions: anchor to the pre-transpose extent on the sheet.
        if part_idx == 1:
            region_row_start = max(1, row_offset - header_rows)
            region_header_rows = header_rows
            if orientation == "vertical" and physical_row_count is not None:
                region_row_end = row_offset + physical_row_count - 1
            else:
                region_row_end = row_offset + e
        else:
            region_row_start = row_offset + s
            region_header_rows = 0
            region_row_end = row_offset + e

        source_ref = SourceRef(
            sheet_name=sheet_name,
            range_a1=range_a1_func(region_row_start, region_row_end, col_start, col_end),
            row_start=region_row_start,
            row_end=region_row_end,
            col_start=col_start,
            col_end=col_end,
        )
        warnings = _build_table_warnings(part_rows, column_schemas, extra_warnings if part_idx == 1 else ())
        profile = TableProfile(
            row_count=len(part_rows),
            column_count=len(column_schemas),
            empty_row_pct=_empty_row_pct(part_rows),
            warnings=warnings,
            columns=column_profiles,
        )
        region_id = table_id_prefix if part_idx == 1 else f"{table_id_prefix}__part{part_idx}"
        regions.append(
            TableRegion(
                table_id=region_id,
                sheet_name=sheet_name,
                orientation=orientation,
                header_rows=region_header_rows,
                row_count=len(part_rows),
                column_count=len(column_schemas),
                source_ref=source_ref,
                columns=column_schemas,
                rows=part_rows,
                profile=profile,
            )
        )

    if not regions:
        # Empty data — keep one empty region so downstream code has something to anchor to.
        empty_schemas = [
            ColumnSchema(
                index=i,
                name_raw=raw,
                name_normalized=norm,
                inferred_type=ColumnType.empty,
                missing_pct=0.0,
                units=u,
            )
            for i, (raw, norm, u) in enumerate(
                zip(column_names_raw, column_names_normalized, column_units, strict=True)
            )
        ]
        empty_profiles = [
            ColumnProfile(name=norm, inferred_type=ColumnType.empty, missing_pct=0.0)
            for norm in column_names_normalized
        ]
        source_ref = SourceRef(
            sheet_name=sheet_name,
            range_a1=range_a1_func(max(1, row_offset - header_rows), max(1, row_offset), col_start, col_end),
            row_start=max(1, row_offset - header_rows),
            row_end=max(1, row_offset),
            col_start=col_start,
            col_end=col_end,
        )
        regions.append(
            TableRegion(
                table_id=table_id_prefix,
                sheet_name=sheet_name,
                orientation=orientation,
                header_rows=header_rows,
                row_count=0,
                column_count=len(empty_schemas),
                source_ref=source_ref,
                columns=empty_schemas,
                rows=[],
                profile=TableProfile(
                    row_count=0,
                    column_count=len(empty_schemas),
                    empty_row_pct=0.0,
                    warnings=list(extra_warnings),
                    columns=empty_profiles,
                ),
            )
        )

    if leftovers_rows:
        primary = regions[0]
        leftover_columns = [
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
        leftovers_region = TableRegion(
            table_id=f"{table_id_prefix}__leftovers",
            sheet_name=sheet_name,
            orientation="leftovers",
            header_rows=0,
            row_count=len(leftovers_rows),
            column_count=2,
            source_ref=primary.source_ref.model_copy(),
            columns=leftover_columns,
            rows=leftovers_rows,
            profile=TableProfile(
                row_count=len(leftovers_rows),
                column_count=2,
                empty_row_pct=0.0,
                warnings=["leftovers_from_vertical_orientation"],
                columns=[
                    ColumnProfile(name="row_index", inferred_type=ColumnType.number, missing_pct=0.0),
                    ColumnProfile(name="cells", inferred_type=ColumnType.mixed, missing_pct=0.0),
                ],
            ),
        )
        primary.profile.warnings.append("has_leftovers_region")
        regions.append(leftovers_region)

    return regions
