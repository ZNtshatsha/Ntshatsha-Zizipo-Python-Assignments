from __future__ import annotations

from typing import Optional

from mongoengine import Document, EmbeddedDocument, fields


class AuthorEmbedded(EmbeddedDocument):
    full_name: str = fields.StringField(required=True, max_length=255)
    title: Optional[str] = fields.StringField()

    def __repr__(self) -> str:
        return f"<AuthorEmbedded {self.full_name}>"


class ScientificArticleDoc(Document):
    meta = {
        "collection": "scientific_articles",
        "indexes": [
            {
                "fields": ["$title", "$summary", "$text"],
                "default_language": "english",
            }
        ],
    }

    title: str = fields.StringField(required=True, max_length=1024)
    summary: Optional[str] = fields.StringField()
    file_path: str = fields.StringField(required=True)
    arxiv_id: str = fields.StringField(required=True, unique=True)
    author: AuthorEmbedded = fields.EmbeddedDocumentField(AuthorEmbedded)
    text: Optional[str] = fields.StringField()
