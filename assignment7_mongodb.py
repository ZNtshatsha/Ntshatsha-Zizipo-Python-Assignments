from __future__ import annotations

import os

from mongoengine import connect, disconnect


def init_mongo() -> None:
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/articles_db")
    connect(host=uri)


def close_mongo() -> None:
    disconnect()
