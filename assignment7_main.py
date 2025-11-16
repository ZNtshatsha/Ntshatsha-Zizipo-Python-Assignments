from __future__ import annotations

from pprint import pprint
from typing import List

from assignment7_csv_to_mariadb import load_csv_to_mariadb
from assignment7_mariadb_to_mongodb import transfer_mariadb_to_mongo
from assignment7_search_mongodb import search_text


def run_pipeline_and_search() -> None:
    print("1) Loading CSV to MariaDB...")
    load_csv_to_mariadb()
    print("=> Done.")

    print("2) Transferring data from MariaDB to MongoDB (converting PDFs)...")
    transfer_mariadb_to_mongo()
    print("=> Done.")

    print("3) Running a sample search on MongoDB...")
    results: List = search_text("transformer")
    print(f"Found {len(results)} results.")
    for res in results:
        pprint(
            {
                "title": res.title,
                "arxiv_id": res.arxiv_id,
                "author": getattr(res.author, "full_name", None),
            }
        )


if __name__ == "__main__":
    run_pipeline_and_search()
