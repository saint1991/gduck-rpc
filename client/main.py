from pathlib import Path

from gduck.client import Connection

if __name__ == "__main__":
    with Connection("localhost:50051").transaction(database_file="datasets/example.duckdb", mode="read_write") as trans:
        result = trans.query_rows("SELECT * FROM videos WHERE comment_count > ? LIMIT 10;", 10)
        print(f"{result} ({type(result)})")
