# gduck Python client

Client implementation to utilize remove gduck server.

## Usage

```python
from gduck import Connection

conn = Connection("localhost:50051")

with conn.transaction(database_file="database.duckdb", mode="read_write") as trans:
    counts = trans.query_value("SELECT COUNT(*) FROM videos WHERE comments > ?", 10)
```
