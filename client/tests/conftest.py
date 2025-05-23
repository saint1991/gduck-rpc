from typing import Generator

import pytest
from gduck.client import Addr, Connection, DuckDbTransaction
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for

IMAGE_NAME = "gduck:latest"
CONTAINER_PORT = 50051


@pytest.fixture(scope="function")
def gduck_container(image_name: str = IMAGE_NAME) -> Generator[DockerContainer, None, None]:
    with DockerContainer(image_name).with_exposed_ports(CONTAINER_PORT) as dc:
        wait_for(lambda: dc.get_wrapped_container() is not None and dc.get_wrapped_container().health == "healthy")
        yield dc


@pytest.fixture(scope="function")
def gduck_connection(gduck_container: DockerContainer) -> Connection:
    host = gduck_container.get_container_host_ip()
    port = gduck_container.get_exposed_port(CONTAINER_PORT)
    return Connection(addr=Addr(host=host, port=port))

@pytest.fixture(scope="function")
def gduck_in_memory_rw_connection(gduck_connection: Connection) -> Generator[DuckDbTransaction, None, None]:
    with gduck_connection.transaction(":memory:", "read_write") as trans:
        yield trans

@pytest.fixture(scope="function")
def gduck_in_memory_ro_connection(gduck_connection: Connection) -> Generator[DuckDbTransaction, None, None]:
    with gduck_connection.transaction(":memory:", "read_only") as trans:
        yield trans
