# usecases/mariadb_to_mongodb.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from assignment7_mongo_models import AuthorEmbedded, ScientificArticleDoc
from assignment7_mongodb import init_mongo
from assignment7_sql_models import Author, ScientificArticle
from assignment7_storage_mariadb import get_session
from pypdf import PdfReader
from sqlalchemy.orm import Session


def _pdf_to_text_markdown(pdf_path: Path) -> str:
    """
    Extract text from PDF using pypdf and do a minimal conversion to Markdown.
    This function is intentionally simple: it returns extracted plain text with
    paragraph splitting (double newlines) to be valid Markdown.
    """
    if not pdf_path.exists():
        return ""
    reader = PdfReader(str(pdf_path))
    pages_text = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if t:
            # normalize whitespace
            t = "\n".join(line.strip() for line in t.splitlines() if line.strip())
            pages_text.append(t)
    # join pages with double newline to create paragraphs
    full_text = "\n\n".join(pages_text)
    return full_text


def transfer_mariadb_to_mongo() -> None:
    """Read articles from MariaDB, convert PDFs to markdown-like text, and save to MongoDB."""
    init_mongo()
    for session in get_session():
        sess: Session = session
        # Query all articles and their authors
        articles = sess.query(ScientificArticle).all()
        for art in articles:
            # Retrieve author record
            author_record: Optional[Author] = (
                sess.query(Author).filter(Author.id == art.author_id).one_or_none()
            )
            author_embedded = None
            if author_record:
                author_embedded = AuthorEmbedded(
                    full_name=author_record.full_name, title=author_record.title
                )

            # Convert PDF to markdown-like text
            pdf_path = Path(art.file_path)
            # In case file path is relative to project root
            if not pdf_path.exists():
                pdf_path = Path(os.getcwd()) / art.file_path
            text_md = _pdf_to_text_markdown(pdf_path)

            # Create or update Mongo document
            existing = ScientificArticleDoc.objects(arxiv_id=art.arxiv_id).first()
            if existing:
                existing.title = art.title
                existing.summary = art.summary
                existing.file_path = art.file_path
                existing.author = author_embedded
                existing.text = text_md
                existing.save()
            else:
                doc = ScientificArticleDoc(
                    title=art.title,
                    summary=art.summary,
                    file_path=art.file_path,
                    arxiv_id=art.arxiv_id,
                    author=author_embedded,
                    text=text_md,
                )
                doc.save()
        break
