from __future__ import annotations
from typing import Dict, Any
import pandas as pd
from sqlalchemy.orm import Session
from assignment7_sql_models import Author, ScientificArticle
from assignment7_storage_mariadb import SessionLocal, init_db


def _insert_row(sess: Session, row: pd.Series) -> Dict[str, Any]:
    """Insert a single row into MariaDB. Return ids."""
    # Find or create author
    name = row["author_full_name"] or ""
    title = row.get("author_title", "") or ""
    author = sess.query(Author).filter(Author.full_name == name).one_or_none()
    if author is None:
        author = Author(full_name=str(name), title=str(title))
        sess.add(author)
        sess.flush()  # populate author.id
    article = sess.query(ScientificArticle).filter(ScientificArticle.arxiv_id == row["arxiv_id"]).one_or_none()
    if article is None:
        article = ScientificArticle(
            title=str(row["title"]),
            summary=str(row.get("summary", "")),
            file_path=str(row.get("file_path", "")),
            arxiv_id=str(row["arxiv_id"]),
            author_id=author.id,
        )
        sess.add(article)
        sess.flush()
    sess.commit()
    return {"author_id": author.id, "article_id": article.id}


def save_dataframe_to_mariadb(df: pd.DataFrame) -> pd.DataFrame:
    """Save df rows to MariaDB and concatenate returned ids to DataFrame."""
    init_db()
    sess = SessionLocal()
    try:
        # apply row-wise, returning dicts of ids
        def _apply_fn(row: pd.Series) -> pd.Series:
            ids = _insert_row(sess, row)
            return pd.Series(ids)

        ids_df = df.apply(_apply_fn, axis=1)
        # concatenate the ids
        df = pd.concat([df.reset_index(drop=True), ids_df.reset_index(drop=True)], axis=1)
        # cast id columns to string dtype for consistency
        df["author_id"] = df["author_id"].astype("Int64").astype("string")
        df["article_id"] = df["article_id"].astype("Int64").astype("string")
        return df
    finally:
        sess.close()