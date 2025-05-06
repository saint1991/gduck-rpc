from decimal import Decimal

from gduck.client import DuckDbTransaction
from pytest_benchmark.fixture import BenchmarkFixture


def test_select_1(gduck_in_memory_rw_connection: DuckDbTransaction) -> None:
    actual = gduck_in_memory_rw_connection.query_value("SELECT 1;")
    assert actual == 1
    assert type(actual) is int

def test_select_1_bench(benchmark: BenchmarkFixture, gduck_in_memory_rw_connection: DuckDbTransaction) -> None:
    benchmark(gduck_in_memory_rw_connection.query_value, "SELECT 1;")

def test_select_1_1(gduck_in_memory_rw_connection: DuckDbTransaction) -> None:
    actual = gduck_in_memory_rw_connection.query_value("SELECT 1.1;")
    assert actual == Decimal("1.1")
    assert type(actual) is Decimal

def test_select_1_1_bench(benchmark: BenchmarkFixture, gduck_in_memory_rw_connection: DuckDbTransaction) -> None:
    benchmark(gduck_in_memory_rw_connection.query_value, "SELECT 1.1;")