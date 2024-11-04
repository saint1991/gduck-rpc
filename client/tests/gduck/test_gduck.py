from gduck.client import Connection


def test_query_value(gduck_connection: Connection) -> None:
    with gduck_connection.transaction(":memory:", "read_write") as trans:
        result = trans.query_value("SELECT 1;")
        assert result == 1
