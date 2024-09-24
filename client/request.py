from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Literal, TypeAlias

from database_pb2 import Connect, DataType
from database_pb2 import Decimal as ProtoDecimal
from database_pb2 import Params, Row, Rows, ScalarValue, Schema
from error_pb2 import Error, ErrorCode
from google.protobuf.struct_pb2 import NULL_VALUE
from location_pb2 import Location
from query_pb2 import Query

__all__ = ["ConnectionMode", "connect"]

ConnectionMode = Literal["auto", "read_write", "read_only"]
Value: TypeAlias = bool | int | float | Decimal | str | datetime | date | time | timedelta | None


def _mode(m: ConnectionMode = Connect.Mode.MODE_AUTO) -> Connect.Mode:
    if m == "read_write":
        return Connect.Mode.MODE_READ_WRITE
    elif m == "read_only":
        return Connect.Mode.MODE_READ_ONLY
    else:
        return Connect.Mode.MODE_AUTO


def connect(file_name: str, mode: ConnectionMode) -> Connect:
    return Connect(file_name=file_name, mode=_mode(mode))


def _value(v: Value) -> ScalarValue:
    if v is None:
        return ScalarValue(null_value=NULL_VALUE)
    elif type(v) is bool:
        return ScalarValue(bool_value=v)
    elif type(v) is int:
        return ScalarValue(int_value=v)
    elif type(v) is float:
        return ScalarValue(double_value=v)
    elif type(v) is Decimal:
        return ScalarValue(decimal_value=ProtoDecimal(value=str(v)))
    elif type(v) is str:
        return ScalarValue(str_value=v)
