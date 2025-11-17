from __future__ import annotations
from typing import Optional
import pandas as pd

from assignment7_storage.mongodb import init_mongo
from assignment7_models.mongo_models import AuthorEmbedded, ScientificArticleDoc


def save_dataframe_to_mongodb(df: pd.DataFrame) -> None:
    """Save DataFrame rows to MongoDB. Use mariadb ids already stored in df."""
    init_mongo()
    # Use apply to insert or update
    def _apply_save(row: pd.Series) -> None:
        author = AuthorEmbedded(full_name=str(row["author_full_name"]), title=str(row.get("author_title", "")))
        text = str(row.get("text_content", "") or "")
        # Use arxiv_id to upsert
        existing = ScientificArticleDoc.objects(arxiv_id=str(row["arxiv_id"])).first()
        if existing:
            existing.title = str(row["title"])
            existing.summary = str(row.get("summary", ""))
            existing.file_path = str(row.get("file_path", ""))
            existing.author = author
            existing.text = text
            existing.mariadb_article_id = str(row.get("article_id", ""))  # optional extra field
            existing.save()
        else:
            doc = ScientificArticleDoc(
                title=str(row["title"]),
                summary=str(row.get("summary", "")),
                file_path=str(row.get("file_path", "")),
                arxiv_id=str(row["arxiv_id"]),
                author=author,
                text=text,
            )
            # optional: save mariadb ids if you want to keep traceability
            try:
                doc.mariadb_article_id = str(row.get("article_id", ""))
            except Exception:
                pass
            doc.save()

    df.apply(_apply_save, axis=1)
