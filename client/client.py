from __future__ import annotations

import threading
from dataclasses import dataclass
from queue import SimpleQueue
from types import TracebackType
from typing import Self

import grpc
import request as proto
from grpc._channel import _MultiThreadedRendezvous
from request import ConnectionMode, Value
from service_pb2 import Request
from service_pb2_grpc import DbServiceStub


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
                self._out.put(response.result)
        except _MultiThreadedRendezvous as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                raise e


class DuckDbTransaction:

    _END_STREAM = "END_STREAM"

    def __init__(self, addr: Addr, database_file: str, mode: ConnectionMode) -> None:
        self._addr = addr
        self._database_file = database_file
        self._mode = mode

        self._requests = SimpleQueue()
        self._results = SimpleQueue()

    # This block executed in other thread
    def _request_generator(self):
        yield self._connect_request()

        while (request := self._requests.get()) != self._END_STREAM:
            yield request

    def execute(self, query: str, *params: tuple[Value]) -> None:
        self._requests.put(proto.request(proto.execute(query, *params)))
        self._results.get()

    def query_value(self, query: str, *params: tuple[Value]) -> Value:
        self._requests.put(proto.request(proto.value(query, *params)))
        result = self._results.get()
        # TODO

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
        return proto.request(kind=proto.connect(file_name=self._database_file, mode=self._mode))
