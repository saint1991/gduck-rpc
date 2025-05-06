from __future__ import annotations

from grpc import RpcError

from .proto.error_pb2 import Error as ProtoError


class GduckException(Exception):
    """Base class for all exceptions raised by gduck."""
    pass

class GduckServerError(GduckException):
    """Raised when the gduck server returns an error."""
    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.code = code

    @classmethod
    def from_proto(cls, error: ProtoError) -> GduckServerError:
        return cls(message=error.message, code=error.code)

class GduckRpcError(GduckException):
    """Raised when there is an error with the gRPC connection."""
    def __init__(self, error: RpcError) -> None:
        super().__init__(str(error))
        self.raw = error
