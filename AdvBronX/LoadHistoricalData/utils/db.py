import pyodbc
from typing import List, Any


def get_connection(conn_str: str) -> pyodbc.Connection:
    """Create and return SQL Server connection."""
    return pyodbc.connect(conn_str)


def batch_insert(
    conn_str: str,
    insert_sql: str,
    rows: List[List[Any]],
    batch_size: int = 5000
) -> None:
    """Insert rows into SQL Server in batches."""
    conn = None
    cursor = None

    try:
        conn = get_connection(conn_str)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        total_rows = len(rows)

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = rows[start:end]

            cursor.executemany(insert_sql, batch)
            conn.commit()

            print(f"Inserted rows {start + 1} to {end} of {total_rows}")

        print("Data inserted successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Load failed: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()