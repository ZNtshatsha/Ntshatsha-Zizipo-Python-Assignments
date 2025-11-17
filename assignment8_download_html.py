from __future__ import annotations
from typing import Optional
import requests
import pandas as pd
from bs4 import BeautifulSoup


def download_html(url: str, timeout: int = 10) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "python-requests/0.0"})
        r.raise_for_status()
        return r.text
    except Exception:
        return ""


def add_html_column(df: pd.DataFrame, url_col: str = "abs_html_url", out_col: str = "html_content") -> pd.DataFrame:
    """Add a column with downloaded HTML using DataFrame.apply (row-wise)."""
    df[out_col] = df[url_col].apply(lambda u: download_html(u if pd.notna(u) else ""))
    return df