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


class ColumnProfile(BaseModel):
    name: str
    inferred_type: ColumnType
    missing_pct: float = Field(ge=0.0, le=100.0)
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_avg: float | None = None
    top_values: list[dict[str, Any]] = Field(default_factory=list)


class TableProfile(BaseModel):
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0)
    empty_row_pct: float = Field(ge=0.0, le=100.0)
    warnings: list[str] = Field(default_factory=list)
    columns: list[ColumnProfile]


class TableRegion(BaseModel):
    table_id: str
    sheet_name: str
    header_rows: int = Field(ge=1)
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
