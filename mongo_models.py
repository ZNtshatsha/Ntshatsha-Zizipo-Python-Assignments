from mongoengine import Document, EmbeddedDocument, fields

class ScientificArticleDoc(Document):
    meta = {
        "collection": "scientific_articles",
        "indexes": [
            {"fields": ["$title", "$summary", "$text"], "default_language": "english"}
        ],
    }
    title = fields.StringField(required=True)
    summary = fields.StringField()
    file_path = fields.StringField()
    arxiv_id = fields.StringField(required=True, unique=True)
    author = fields.EmbeddedDocumentField(AuthorEmbedded)
    text = fields.StringField()
    mariadb_article_id = fields.StringField()