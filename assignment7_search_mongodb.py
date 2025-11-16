from __future__ import annotations

from typing import List

from assignment7_mongo_models import ScientificArticleDoc
from assignment7_mongodb import init_mongo


def search_text(query: str, limit: int = 10) -> List[ScientificArticleDoc]:
    """Search MongoDB text index and return matching documents."""
    init_mongo()
    # MongoEngine provides search_text for text indexes.
    qs = (
        ScientificArticleDoc.objects.search_text(query)
        .order_by("$text_score")
        .limit(limit)
    )
    return list(qs)
