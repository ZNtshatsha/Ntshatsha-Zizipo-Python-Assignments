from __future__ import annotations
from typing import List
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
import pandas as pd

ARXIV_API = "http://export.arxiv.org/api/query?search_query={query}&start=0&max_results={max_results}"


def _parse_entry(entry: ET.Element) -> dict:
    # arXiv XML uses the Atom namespace; handle tags accordingly
    ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
    # title
    title = entry.find("atom:title", ns).text or ""
    summary = entry.find("atom:summary", ns).text or ""
    # id element contains the arXiv URL like 'http://arxiv.org/abs/2301.12345v1'
    id_url = entry.find("atom:id", ns).text or ""
    # extract arxiv_id (strip version suffix)
    arxiv_id = id_url.rsplit("/", 1)[-1].split("v")[0]
    # authors: take first author_full_name
    author_el = entry.find("atom:author/atom:name", ns)
    author_full_name = (author_el.text or "") if author_el is not None else ""
    # author title is not provided by arXiv; leave empty
    return {
        "title": title.strip(),
        "summary": summary.strip(),
        "file_path": "",  # no local pdf by default
        "arxiv_id": arxiv_id.strip(),
        "author_full_name": author_full_name.strip(),
        "author_title": "",
        "abs_html_url": id_url.strip(),
    }


def fetch_arxiv_to_dataframe(query: str, max_results: int = 10) -> pd.DataFrame:
    """Fetch small number of results from arXiv API and return DataFrame with the same columns + abs_html_url."""
    q = quote(query)
    url = ARXIV_API.format(query=q, max_results=max_results)
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    entries = root.findall("{http://www.w3.org/2005/Atom}entry")
    rows: List[dict] = [_parse_entry(e) for e in entries]
    df = pd.DataFrame.from_records(rows)
    # ensure same dtype and columns as CSV; keep abs_html_url as extra column
    expected = ["title", "summary", "file_path", "arxiv_id", "author_full_name", "author_title"]
    for col in expected:
        if col not in df:
            df[col] = ""
    df = df.astype({c: "string" for c in expected})
    # keep abs_html_url column as string too
    df["abs_html_url"] = df["abs_html_url"].astype("string")
    cols = expected + ["abs_html_url"]
    return df[cols]
