from __future__ import annotations
from typing import List
from assignment_7_mongo_models import ScientificArticleDoc
from assignment7_storage.mongodb import init_mongo

def search_text(query: str, limit: int = 10) -> List[ScientificArticleDoc]:
    init_mongo()
    qs = ScientificArticleDoc.objects.search_text(query).order_by("$text_score").limit(limit)
    return list(qs)
