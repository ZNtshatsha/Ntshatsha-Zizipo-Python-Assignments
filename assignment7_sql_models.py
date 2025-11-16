from __future__ import annotations

from typing import Any

from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Author(Base):
    __tablename__ = "authors"

    id: Any = Column(Integer, primary_key=True, autoincrement=True)
    full_name: str = Column(String(255), nullable=False)
    title: str = Column(String(255), nullable=True)

    articles = relationship("ScientificArticle", back_populates="author")


class ScientificArticle(Base):
    __tablename__ = "scientific_articles"

    id: Any = Column(Integer, primary_key=True, autoincrement=True)
    title: str = Column(String(1024), nullable=False)
    summary: str = Column(Text, nullable=True)
    file_path: str = Column(String(1024), nullable=False)
    arxiv_id: str = Column(String(64), unique=True, nullable=False)

    author_id: Any = Column(Integer, ForeignKey("authors.id"), nullable=False)
    author = relationship("Author", back_populates="articles")
