from client import Connection

if __name__ == "__main__":
    with Connection("localhost:50051").transaction(database_file="example.duckdb", mode="read_write") as trans:
        result = trans.query("SELECT '1'")
        print(result)
