import os
from dotenv import load_dotenv

from utils.helpers import (
    load_json,
    get_filename,
    validate_required_columns,
    build_insert_sql
)
from utils.excel import (
    read_excel_as_string,
    clean_dataframe,
    add_filename_column,
    reorder_columns
)
from utils.db import batch_insert


def main():
    # Load environment variables
    load_dotenv(r"C:\code\PythonETL\BronXETL\.env")

    excel_file = os.getenv("excel_file")
    sheet_name = os.getenv("sheet_name")
    json_config_path = os.getenv("json_config_path", r"C:\code\PythonETL\BronXETL\config\bronx_columns.json")

    if not excel_file:
        raise ValueError("excel_file is missing in .env")

    filename = get_filename(excel_file)

    # Load JSON config
    config = load_json(json_config_path)
    table_name = config["table_name"]
    columns = config["columns"]

    # Build connection string
    conn_str = f"""
    DRIVER={{ODBC Driver 18 for SQL Server}};
    SERVER={os.getenv("server")};
    DATABASE={os.getenv("database")};
    UID={os.getenv("username")};
    PWD={os.getenv("password")};
    Encrypt=yes;
    TrustServerCertificate=yes;
    Connection Timeout=30;
    """

    # Read and prepare Excel
    df = read_excel_as_string(excel_file, sheet_name=sheet_name)
    df = add_filename_column(df, filename)
    df = clean_dataframe(df)

    # Validate and reorder columns
    validate_required_columns(df.columns.tolist(), columns)
    df = reorder_columns(df, columns)

    # Build insert SQL
    insert_sql = build_insert_sql(table_name, columns)

    # Convert rows for SQL insert
    rows = df.values.tolist()

    # Insert
    batch_insert(conn_str, insert_sql, rows, batch_size=5000)


if __name__ == "__main__":
    main()