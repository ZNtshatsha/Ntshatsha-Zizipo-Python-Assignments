from __future__ import annotations
from pathlib import Path
from typing import Final

import pandas as pd

CSV_PATH: Final[Path] = Path("assignment7_data/assignmentc7_articles.csv")


def load_csv_to_dataframe(csv_path: Path | None = None) -> pd.DataFrame:
    """Load CSV into pandas DataFrame with explicit string dtype for all columns."""
    path = csv_path or CSV_PATH
    df = pd.read_csv(path, dtype=str) 
    # Ensure all columns exist and convert to string dtype
    expected = ["title", "summary", "file_path", "arxiv_id", "author_full_name", "author_title"]
    for col in expected:
        if col not in df.columns:
            df[col] = ""  # create missing columns
    # Convert to pandas string dtype
    df = df.astype({c: "string" for c in expected})
    return df[expected]
