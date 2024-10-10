from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Callable, Self, TypeAlias

from database_pb2 import Column as ProtoColumn
from database_pb2 import DataType, Rows, ScalarValue
from database_pb2 import Schema as ProtoSchema
from dateutil.relativedelta import relativedelta
from location_pb2 import Location

from .types import Value

__all__ = ["parse_value", "parse_rows", "parse_location"]

ParquetLocation: TypeAlias = Path


@dataclass(frozen=True)
class Column:
    name: str
    data_type: DataType

    @classmethod
    def from_proto(cls, col: ProtoColumn) -> Self:
        cls(name=col.name, data_type=col.data_type)


@dataclass(frozen=True)
class Schema:
    columns: list[Column]

    def names(self) -> list[str]:
        return [col.name for col in self.columns]

    def value_getters(self) -> list[Callable[[ScalarValue], Value]]:
        ret = []

        for col in self.columns:
            if col.data_type == DataType.DATATYPE_NULL:
                ret.append(_null_value)
            elif col.data_type == DataType.DATATYPE_BOOL:
                ret.append(_bool_value)
            elif col.data_type in (DataType.DATATYPE_INT, DataType.DATATYPE_UINT):
                ret.append(_int_value)
            elif col.data_type == DataType.DATATYPE_DOUBLE:
                ret.append(_double_value)
            elif col.data_type == DataType.DATATYPE_DECIMAL:
                ret.append(_decimal_value)
            elif col.data_type == DataType.DATATYPE_STRING:
                ret.append(_str_value)
            elif col.data_type == DataType.DATATYPE_DATETIME:
                ret.append(_datetime_value)
            elif col.data_type == DataType.DATATYPE_DATE:
                ret.append(_date_value)
            elif col.data_type == DataType.DATATYPE_TIME:
                ret.append(_time_value)
            elif col.data_type == DataType.DATATYPE_INTERVAL:
                ret.append(_interval_value)
            else:
                raise ValueError(f"unknown data type at column {col.name}")

        return ret

    @classmethod
    def from_proto(cls, schema: ProtoSchema) -> Self:
        columns = [Column.from_proto(col) for col in schema.columns]
        return cls(columns)


def _null_value(v: ScalarValue) -> None:
    return None


def _bool_value(v: ScalarValue) -> bool:
    return v.bool_value


def _int_value(v: ScalarValue) -> int:
    return v.int_value


def _uint_value(v: ScalarValue) -> int:
    return v.uint_value


def _double_value(v: ScalarValue) -> float:
    return v.double_value


def _decimal_value(v: ScalarValue) -> Decimal:
    return Decimal(v.decimal_value.value)


def _str_value(v: ScalarValue) -> str:
    return v.str_value


def _datetime_value(v: ScalarValue) -> datetime:
    return v.datetime_value.ToDatetime()


def _date_value(v: ScalarValue) -> date:
    dt = v.date_value
    return date(year=dt.year, month=dt.month, day=dt.day)


def _time_value(v: ScalarValue) -> time:
    t = v.time_value
    return time(
        hour=t.hours, minute=t.minutes, second=t.seconds, microsecond=int(t.nanos / 1000)
    )  # FIXME python time does not support nanosecond precision


def _interval_value(v: ScalarValue) -> relativedelta:
    interval = v.interval_value
    return relativedelta(
        months=interval.months, days=interval.days, microseconds=int(interval.nanos / 1000)
    )  # FIXME python relativedelta does not support nanosecond precision


def parse_value(v: ScalarValue) -> Value:
    if v.HasField("null_value"):
        return None
    elif v.HasField("bool_value"):
        return _bool_value(v)
    elif v.HasField("int_value"):
        return _int_value(v)
    elif v.HasField("uint_value"):
        return _uint_value(v)
    elif v.HasField("double_value"):
        return _double_value(v)
    elif v.HasField("decimal_value"):
        return _decimal_value(v)
    elif v.HasField("str_value"):
        return _str_value(v)
    elif v.HasField("datetime_value"):
        return _datetime_value(v)
    elif v.HasField("date_value"):
        return _date_value(v)
    elif v.HasField("time_value"):
        return _time_value(v)
    elif v.HasField("interval_value"):
        return _interval_value(v)
    else:
        raise ValueError(f"unknown type of value {v}")


def parse_rows(rows: Rows) -> tuple[Schema, list[list[Value]]]:
    schema = Schema.from_proto(rows.schema)

    getters = enumerate(schema.value_getters())

    ret = []
    for row in rows.rows:
        ret.append([getter(row[i]) for i, getter in getters])

    return schema, ret


def parse_location(location: Location) -> ParquetLocation:
    return Path(location.local.path)
