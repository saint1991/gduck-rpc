from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Self, TypeAlias

from database_pb2 import Column as ProtoColumn
from database_pb2 import DataType
from database_pb2 import Schema as ProtoSchema
from dateutil.relativedelta import relativedelta

Value: TypeAlias = bool | int | float | Decimal | str | datetime | date | time | relativedelta | None


@dataclass(frozen=True)
class Column:
    name: str
    data_type: DataType

    @classmethod
    def from_proto(cls, col: ProtoColumn) -> Self:
        return cls(name=col.name, data_type=col.data_type)


@dataclass(frozen=True)
class Schema:
    columns: list[Column]

    def __iter__(self) -> Iterator[Column]:
        return iter(self.columns)

    def __getitem__(self, key: int) -> Column:
        return self.columns[key]

    def names(self) -> list[str]:
        return [col.name for col in self.columns]

    @classmethod
    def from_proto(cls, schema: ProtoSchema) -> Self:
        columns = [Column.from_proto(col) for col in schema.columns]
        return cls(columns)


@dataclass(frozen=True)
class Rows:
    schema: Schema
    rows: list[tuple[Value]]

    def __iter__(self) -> Iterator[tuple[Value]]:
        return iter(self.rows)


ParquetLocation: TypeAlias = Path
