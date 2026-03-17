import pandas as pd
from typing import Optional


def read_excel_as_string(excel_file: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Read Excel file into DataFrame as string."""
    if sheet_name:
        df = pd.read_excel(
            excel_file,
            sheet_name=sheet_name,
            engine="openpyxl",
            dtype=str
        )
    else:
        df = pd.read_excel(
            excel_file,
            engine="openpyxl",
            dtype=str
        )

    return df.astype(str)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Standard cleanup for ETL load."""
    df = df.replace("nan", None)
    df = df.replace("", None)
    return df


def add_filename_column(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Append filename column."""
    df["Filename"] = filename
    return df


def reorder_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Reorder DataFrame to exactly match SQL insert columns."""
    return df[columns]