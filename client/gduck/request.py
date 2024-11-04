import math
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Literal

from dateutil.relativedelta import relativedelta
from google.protobuf.struct_pb2 import NULL_VALUE
from google.protobuf.timestamp_pb2 import Timestamp

from .proto.database_pb2 import Connect, Date
from .proto.database_pb2 import Decimal as ProtoDecimal
from .proto.database_pb2 import Interval, Params, ScalarValue, Time
from .proto.location_pb2 import Location
from .proto.query_pb2 import Query
from .proto.service_pb2 import Request
from .types import Value

__all__ = ["ConnectionMode", "connect", "local_file", "execute", "value", "rows", "ctas", "parquet", "request"]

ConnectionMode = Literal["auto", "read_write", "read_only"]


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
    elif type(v) is datetime:
        fraction, seconds = math.modf(v.timestamp())
        return ScalarValue(datetime_value=Timestamp(seconds=int(seconds), nanos=int(fraction * 1000000000)))
    elif type(v) is date:
        return ScalarValue(date_value=Date(year=v.year, month=v.month, day=v.day))
    elif type(v) is time:
        return ScalarValue(time_value=Time(hours=v.hour, minutes=v.minute, seconds=v.second, nanos=v.microsecond * 1000))
    elif type(v) is relativedelta:
        nv = v.normalized()
        return ScalarValue(
            interval_value=Interval(
                months=12 * nv.years + nv.months,
                days=nv.days,
                nanos=1000000000 * (nv.hours * 3600 + nv.minutes * 60 + nv.seconds) + 1000 * nv.microseconds,
            )
        )
    else:
        raise ValueError(f"Invalid type of value: {v} ({type(v)})")


def _params(*params: tuple[Value]) -> Params:
    return Params(params=[_value(p) for p in params])


def local_file(path: Path) -> Location:
    return Location(local=Location.LocalFile(path=str(path)))


def execute(query: str, *params: tuple[Value]) -> Query:
    return Query(execute=Query.Execute(query=query, params=_params(*params)))


def value(query: str, *params: tuple[Value]) -> Query:
    return Query(value=Query.QueryValue(query=query, params=_params(*params)))


def rows(query: str, *params: tuple[Value]) -> Query:
    return Query(rows=Query.QueryRows(query=query, params=_params(*params)))


def ctas(table_name: str, query: str, *params: tuple[Value]) -> Query:
    return Query(ctas=Query.CreateTableAsQuery(table_name=table_name, query=query, params=_params(*params)))


def parquet(location: Location, query: str, *params: tuple[Value]) -> Query:
    return Query(parquet=Query.ParquetQuery(location=location, query=query, params=_params(*params)))


def request(kind: Connect | Query) -> Request:
    if type(kind) is Connect:
        return Request(connect=kind)
    elif type(kind) is Query:
        return Request(query=kind)
    else:
        raise ValueError(f"unsupported type of message: {kind}")
