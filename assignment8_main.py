from __future__ import annotations
from pprint import pprint

import pandas as pd

from assignment8_dataframe_loader import load_csv_to_dataframe
from assignment8_arxiv_fetcher import fetch_arxiv_to_dataframe
from assignment8_download_html import add_html_column
from assignment8_html_to_text import add_text_column
from assignment8_to_mariadb import save_dataframe_to_mariadb
from assignment8_to_mongodb import save_dataframe_to_mongodb
from assignment8_search_mongodb import search_text


def run_pipeline() -> None:
    print("Loading CSV into DataFrame")
    csv_df = load_csv_to_dataframe()
    print(f"CSV rows: {len(csv_df)}")

    print("Fetching arXiv data for query 'machine learning' (example)")
    arxiv_df = fetch_arxiv_to_dataframe("machine learning", max_results=5)
    print(f"arXiv rows: {len(arxiv_df)}")

    print("Combine CSV and arXiv DataFrames (deduplicate by arxiv_id)")
    combined = pd.concat([csv_df, arxiv_df], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=["arxiv_id"], keep="first")
    print(f"Combined rows: {len(combined)}")

    print("Download HTML pages")
    combined = add_html_column(combined, url_col="abs_html_url", out_col="html_content")

    print("Extract text from HTML")
    combined = add_text_column(combined, html_col="html_content", text_col="text_content")

    print("Save to MariaDB (will add author_id and article_id to dataframe)")
    combined = save_dataframe_to_mariadb(combined)
    print("Saved to MariaDB.")

    print("Save to MongoDB")
    save_dataframe_to_mongodb(combined)
    print("Saved to MongoDB.")

    print("Run search on MongoDB for 'transformer'")
    results = search_text("transformer", limit=10)
    print("Search results (titles):")
    for r in results:
        pprint({"title": r.title, "arxiv_id": r.arxiv_id})


if __name__ == "__main__":
    run_pipeline()
