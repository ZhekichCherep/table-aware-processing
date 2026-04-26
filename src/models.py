"""Domain models for the table-aware processing pipeline.

All types here are platform-agnostic (no pandas/openpyxl dependencies) so they
can be reused both by parsers and by chunking/serialization layers.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ColumnType(str, Enum):
    string = "string"
    number = "number"
    date = "date"
    datetime = "datetime"
    bool = "bool"
    mixed = "mixed"
    empty = "empty"


class SourceRef(BaseModel):
    """Absolute coordinates of a region/chunk in the source file (1-based)."""

    sheet_name: str
    range_a1: str | None = None
    row_start: int = Field(ge=1)
    row_end: int = Field(ge=1)
    col_start: int = Field(ge=1)
    col_end: int = Field(ge=1)


class ColumnSchema(BaseModel):
    index: int = Field(ge=0)
    name_raw: str
    name_normalized: str
    inferred_type: ColumnType
    missing_pct: float = Field(ge=0.0, le=100.0)
    units: str | None = None  # extracted from headers like "Цена, руб." -> "руб."


class ColumnProfile(BaseModel):
    name: str
    inferred_type: ColumnType
    missing_pct: float = Field(ge=0.0, le=100.0)
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_avg: float | None = None
    date_min: str | None = None
    date_max: str | None = None
    distinct_count: int | None = None
    top_values: list[dict[str, Any]] = Field(default_factory=list)


class TableProfile(BaseModel):
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0)
    empty_row_pct: float = Field(ge=0.0, le=100.0)
    warnings: list[str] = Field(default_factory=list)
    columns: list[ColumnProfile]


class TableRegion(BaseModel):
    """One logical table on a sheet.

    `source_ref` is the absolute bounding box in the source file:
    - For the primary region: includes header rows.
    - For sub-regions split by empty rows: data-only span (no header inside),
      with `header_rows = 0` and headers inherited from `columns`.
    """

    table_id: str
    sheet_name: str
    orientation: str = "horizontal"
    header_rows: int = Field(ge=0)  # 0 allowed for inherited-header sub-regions
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0)
    source_ref: SourceRef
    columns: list[ColumnSchema]
    rows: list[dict[str, Any]]
    profile: TableProfile


class SheetModel(BaseModel):
    sheet_name: str
    table_regions: list[TableRegion]


class NormalizedDocument(BaseModel):
    schema_version: str = "v1"
    source_file: str
    source_format: str
    processed_at: datetime
    sheets: list[SheetModel]


class ChunkModel(BaseModel):
    """RAG-ready chunk.

    `row_start` / `row_end` and `source_ref` reference absolute (sheet) row
    numbers of the data slice this chunk represents (header rows are not
    included in the count — they live in `header_context`).
    """

    chunk_id: str
    source_file: str
    source_format: str
    sheet_name: str
    table_id: str
    row_start: int = Field(ge=1)
    row_end: int = Field(ge=1)
    source_ref: SourceRef
    header_context: list[str]
    columns: list[ColumnSchema]
    records: list[dict[str, Any]]
    text_projection: str
