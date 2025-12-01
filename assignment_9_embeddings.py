import google.generativeai as genai
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from dotenv import load_dotenv
import os
import pandas as pd

# Load API key
load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# 1. Simple phrase embeddings
phrases = [
    "I love data engineering.",
    "Machine learning is fun.",
    "Cats are cute."
]

embeddings = []
for text in phrases:
    response = genai.embed_content(
        model="text-embedding-004",
        content=text,
        task_type="retrieval_document"
    )
    embeddings.append(response["embedding"])

embeddings = np.array(embeddings)

# Cosine similarity
sim_matrix = cosine_similarity(embeddings)
print("Cosine similarity matrix:")
print(sim_matrix)

# 2. Load Articles
ARTICLES_FOLDER = "assignment7_articles"

articles_data = []
for filename in os.listdir(ARTICLES_FOLDER):
    if filename.endswith(".txt"):
        path = os.path.join(ARTICLES_FOLDER, filename)
        with open(path, "r", encoding="utf-8") as f:
            articles_data.append({
                "filename": filename,
                "text": f.read()
            })

df_articles = pd.DataFrame(articles_data)
print("\nLoaded articles:")
print(df_articles)

# 3. Chunk text
def chunk_text(text, chunk_size=300, overlap=50):
    words = text.split()
    chunks = []
    start = 0

    while start < len(words):
        end = start + chunk_size
        chunks.append(" ".join(words[start:end]))
        start = end - overlap
        if start < 0:
            start = 0

    return chunks

all_chunks = []
for _, row in df_articles.iterrows():
    chunks = chunk_text(row["text"], 300, 50)
    for c in chunks:
        all_chunks.append({
            "filename": row["filename"],
            "chunk": c
        })

df_chunks = pd.DataFrame(all_chunks)
print("\nGenerated chunks:")
print(df_chunks.head())

# 4. Embed chunks
chunk_embeddings = []

for chunk in df_chunks["chunk"]:
    emb = genai.embed_content(
        model="text-embedding-004",
        content=chunk
    )["embedding"]
    
    chunk_embeddings.append(emb)

df_chunks["embedding"] = chunk_embeddings

print("\nEmbeddings created for", len(df_chunks), "chunks.")
