from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Callable

from dateutil.relativedelta import relativedelta

from .proto.database_pb2 import DataType, Rows, ScalarValue
from .proto.location_pb2 import Location
from .types import ParquetLocation, Schema, Value

__all__ = ["parse_value", "parse_rows", "parse_location"]


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


def _getter(data_type: DataType) -> Callable[[ScalarValue], Value]:
    if data_type == DataType.DATATYPE_NULL:
        return _null_value
    elif data_type == DataType.DATATYPE_BOOL:
        return _bool_value
    elif data_type in (DataType.DATATYPE_INT, DataType.DATATYPE_UINT):
        return _int_value
    elif data_type == DataType.DATATYPE_DOUBLE:
        return _double_value
    elif data_type == DataType.DATATYPE_DECIMAL:
        return _decimal_value
    elif data_type == DataType.DATATYPE_STRING:
        return _str_value
    elif data_type == DataType.DATATYPE_DATETIME:
        return _datetime_value
    elif data_type == DataType.DATATYPE_DATE:
        return _date_value
    elif data_type == DataType.DATATYPE_TIME:
        return _time_value
    elif data_type == DataType.DATATYPE_INTERVAL:
        return _interval_value
    else:
        raise ValueError(f"unknown data type: {data_type}")


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


def parse_rows(rows: Rows) -> tuple[Schema, list[tuple[Value]]]:
    schema = Schema.from_proto(rows.schema)

    getters = [_getter(col.data_type) for col in schema]

    ret = []
    for row in rows.rows:
        values = tuple(getters[i](v) for i, v in enumerate(row.values))
        ret.append(values)

    return schema, ret


def parse_location(location: Location) -> ParquetLocation:
    return Path(location.local.path)
