import json
import os
from typing import List, Dict, Any


def load_json(file_path: str) -> Dict[str, Any]:
    """Load JSON config file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_filename(file_path: str) -> str:
    """Extract file name from full file path."""
    return os.path.basename(file_path)


def validate_required_columns(df_columns: List[str], required_columns: List[str]) -> None:
    """Raise error if required columns are missing from DataFrame."""
    missing = [col for col in required_columns if col not in df_columns]
    if missing:
        raise ValueError(f"Missing required columns in Excel: {missing}")


def build_insert_sql(table_name: str, columns: List[str]) -> str:
    """Build parameterized INSERT SQL."""
    placeholders = ",".join(["?"] * len(columns))
    column_sql = ",".join([f"[{c}]" for c in columns])

    return f"""
    INSERT INTO {table_name} ({column_sql})
    VALUES ({placeholders})
    """