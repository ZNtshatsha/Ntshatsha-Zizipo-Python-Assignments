from __future__ import annotations
from typing import Optional
import pandas as pd
from bs4 import BeautifulSoup


def html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    # Extract the abstract text specifically if available
    # On an arXiv abs page the abstract is inside <blockquote class="abstract"> ... </blockquote>
    block = soup.find("blockquote", {"class": "abstract"})
    if block:
        return block.get_text(separator="\n").strip()
    # Fallback: return visible body text
    text = soup.get_text(separator="\n")
    return text.strip()


def add_text_column(df: pd.DataFrame, html_col: str = "html_content", text_col: str = "text_content") -> pd.DataFrame:
    df[text_col] = df[html_col].apply(lambda h: html_to_text(h))
    return df
