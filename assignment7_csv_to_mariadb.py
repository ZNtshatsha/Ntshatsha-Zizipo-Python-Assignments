from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from assignment7_sql_models import Author, ScientificArticle
from assignment7_storage_mariadb import get_session, init_db
from sqlalchemy.orm import Session

CSV_PATH = Path("data/articles.csv")


def _row_iter(csv_path: Path) -> Iterable[dict]:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def load_csv_to_mariadb() -> None:
    """Load CSV rows into MariaDB, creating authors and articles."""
    init_db()
    for session in get_session():
        # get_session is a generator; iterate once to enter the try/finally that yields session
        sess: Session = session
        for row in _row_iter(CSV_PATH):
            full_name = row["author_full_name"].strip()
            title = row.get("author_title", "").strip()
            # Find existing author
            author = (
                sess.query(Author).filter(Author.full_name == full_name).one_or_none()
            )
            if author is None:
                author = Author(full_name=full_name, title=title)
                sess.add(author)
                sess.flush()  # populate author.id
            article = ScientificArticle(
                title=row["title"].strip(),
                summary=row.get("summary", "").strip(),
                file_path=row["file_path"].strip(),
                arxiv_id=row["arxiv_id"].strip(),
                author_id=author.id,
            )
            sess.add(article)
        sess.commit()
        break
