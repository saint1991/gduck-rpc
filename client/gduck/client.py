from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from queue import SimpleQueue
from types import TracebackType
from typing import Generator, Self

import grpc
from grpc._channel import _MultiThreadedRendezvous

from .exceptions import GduckRpcError, GduckServerError
from .proto.query_pb2 import Query
from .proto.service_pb2 import Request, Response
from .proto.service_pb2_grpc import DbServiceStub
from .request import ConnectionMode, Value, connect, ctas, execute, local_file, parquet, request, rows, value
from .response import parse_location, parse_rows, parse_value

__all__ = ["Addr", "Connection", "DuckDbTransaction"]


@dataclass(frozen=True)
class Addr:
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"

    @classmethod
    def from_str(cls, addr: str) -> Self:
        parts = addr.split(":")
        port = int(parts[-1])
        host = "".join(parts[0:-1])
        return cls(host, port)


@dataclass(frozen=True)
class Connection:
    addr: Addr

    def transaction(self, database_file: str, mode: ConnectionMode) -> DuckDbTransaction:
        return DuckDbTransaction(self.addr, database_file=database_file, mode=mode)


class ResponseHandlerThread(threading.Thread):

    def __init__(self, responses: _MultiThreadedRendezvous, out: SimpleQueue, group: None = None, name: str | None = None) -> None:
        super().__init__(group=group, name=name or f"{type(self).__name__}-{threading.active_count() + 1}")

        self._responses = responses
        self._out = out

    def run(self) -> None:
        try:
            for response in self._responses:
                if response.HasField("success"):
                    self._out.put(response.success)
                elif response.HasField("error"):
                    self._out.put(GduckServerError.from_proto(response.error))
        except _MultiThreadedRendezvous as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                self._out.put(GduckRpcError(e))


class DuckDbTransaction:

    _END_STREAM = "END_STREAM"

    def __init__(self, addr: Addr, database_file: str, mode: ConnectionMode) -> None:
        self._addr = addr
        self._database_file = database_file
        self._mode = mode

        self._requests = SimpleQueue()
        self._results = SimpleQueue()

    # This block executed in other thread
    def _request_generator(self) -> Generator[Request, None, None]:
        yield self._connect_request()

        while (request := self._requests.get()) != self._END_STREAM:
            yield request

    def _query(self, query: Query) -> Response.QueryResult:
        self._requests.put(request(query))
        result = self._results.get()
        if isinstance(result, Exception):
            raise result
        else:
            return result

    def execute(self, query: str, *params: tuple[Value]) -> None:
        self._query(execute(query, *params))

    def query_value(self, query: str, *params: tuple[Value]) -> Value:
        result = self._query(value(query, *params))
        return parse_value(result.value)

    def query_rows(self, query: str, *params: tuple[Value]) -> Value:
        result = self._query(rows(query, *params))
        _, r = parse_rows(result.rows)
        return r

    def ctas(self, table_name: str, query: str, *params: tuple[Value]) -> None:
        self._query(ctas(table_name, query, *params))

    def local_parquet(self, file: Path, query: str, *params: tuple[Value]) -> Path:
        result = self._query(parquet(local_file(file), query, *params))
        return parse_location(result.parquet_file)

    def __enter__(self) -> Self:
        self._channel = grpc.insecure_channel(target=str(self._addr))

        self._responses: _MultiThreadedRendezvous = DbServiceStub(self._channel).Transaction(self._request_generator())

        self._response_thread = ResponseHandlerThread(responses=self._responses, out=self._results)
        self._response_thread.start()

        return self

    def __exit__(self, exc_type: type, exc_value: Exception, traceback: TracebackType) -> bool:
        self._requests.put(self._END_STREAM)
        self._channel.close()
        return False

    def _connect_request(self) -> Request:
        return request(kind=connect(file_name=self._database_file, mode=self._mode))
