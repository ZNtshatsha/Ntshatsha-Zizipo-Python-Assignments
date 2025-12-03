# qdrant_integration.py
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

# CONFIG - adjust if needed
QDRANT_HOST = os.getenv("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "articles_chunks")

# Connect to local Qdrant
def get_qdrant_client():
    return QdrantClient(url=f"http://{QDRANT_HOST}:{QDRANT_PORT}")

# Pydantic model for metadata associated with each chunk
class ChunkMeta(BaseModel):
    filename: str
    chunk_index: int
    text_preview: Optional[str] = None  
    extra: Optional[Dict[str, Any]] = None

# deterministic point id generator
def make_point_id(filename: str, chunk_index: int) -> str:
    key = f"{filename}::{chunk_index}"

    return hashlib.sha256(key.encode("utf-8")).hexdigest()

# Create collection if missing
def ensure_collection(client: QdrantClient, vector_size: int, distance: rest.Distance = rest.Distance.COSINE):
    collections = client.get_collections().collections
    names = [c.name for c in collections]
    if COLLECTION_NAME not in names:
        print(f"Creating Qdrant collection '{COLLECTION_NAME}' with dim={vector_size}")
        client.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=rest.VectorParams(size=vector_size, distance=distance)
        )
    else:
        print(f"Collection '{COLLECTION_NAME}' already exists")

# Check whether a point exists by id
def chunk_exists(client: QdrantClient, point_id: str) -> bool:
    try:
        # Qdrant get_point returns point or raises if not found
        res = client.get_point(collection_name=COLLECTION_NAME, id=point_id)
        return res is not None
    except Exception:
        return False

# Save a single chunk (point).
def save_chunk(client: QdrantClient, point_id: str, vector: List[float], meta: ChunkMeta):
    point = rest.PointStruct(id=point_id, vector=list(vector), payload=meta.dict())
    client.upsert(collection_name=COLLECTION_NAME, points=[point])

# Bulk upsert for many points (id, vector, meta)
def upsert_chunks(client: QdrantClient, items: List[Dict]):
    """
    items: list of {"id": point_id, "vector": vector_list, "meta": ChunkMeta}
    """
    points = []
    for it in items:
        points.append(rest.PointStruct(
            id=it["id"],
            vector=list(it["vector"]),
            payload=it["meta"].dict()
        ))
    # uses upsert
    client.upsert(collection_name=COLLECTION_NAME, points=points)

# Which chunks from a list of point ids are missing?
def missing_point_ids(client: QdrantClient, point_ids: List[str]) -> List[str]:
    # Qdrant can fetch by ids in batch via "scroll" or get_points - easiest: try get_point in loop (small scale).
    missing = []
    for pid in point_ids:
        if not chunk_exists(client, pid):
            missing.append(pid)
    return missing

# Search: query_text
def semantic_search(client: QdrantClient, query_vector: List[float], top_k: int = 5):
    hits = client.search(
        collection_name=COLLECTION_NAME,
        query_vector=list(query_vector),
        limit=top_k,
        with_payload=True,
        with_vector=False
    )
    # return hits as list
    return hits

# Helper: convenience wrapper to process your df_chunks (pandas) and ensure only missing ones are embedded+uploaded
def process_and_upload_missing(client: QdrantClient, df_chunks, embed_fn, batch_size=32):
    """
    df_chunks must have columns: filename, chunk_index, chunk (text)
    embed_fn(texts: List[str]) -> List[List[float]]
    """
    import math
    # create deterministic ids
    df_chunks = df_chunks.copy()
    df_chunks["point_id"] = df_chunks.apply(
        lambda r: make_point_id(r["filename"], r.get("chunk_index", 0)), axis=1
    )

    # determine which are missing
    point_ids = df_chunks["point_id"].tolist()
    missing_ids = missing_point_ids(client, point_ids)
    if not missing_ids:
        print("All chunks already in Qdrant.")
        return

    # subset to missing rows in same order
    missing_df = df_chunks[df_chunks["point_id"].isin(missing_ids)].reset_index(drop=True)
    print(f"{len(missing_df)} missing chunks to embed and upload")

    # create embeddings in batches for missing chunks
    to_upsert = []
    for start in range(0, len(missing_df), batch_size):
        batch = missing_df.iloc[start:start+batch_size]
        texts = batch["chunk"].tolist()
        vectors = embed_fn(texts)   # should return list[list[float]]
        # ensure collection exists (get dim from first vector)
        if start == 0:
            dim = len(vectors[0])
            ensure_collection(client, vector_size=dim)
        for i, vec in enumerate(vectors):
            row = batch.iloc[i]
            meta = ChunkMeta(
                filename=row["filename"],
                chunk_index=int(row.get("chunk_index", 0)),
                text_preview=(row["chunk"][:200] + "...") if len(row["chunk"])>200 else row["chunk"]
            )
            to_upsert.append({"id": row["point_id"], "vector": vec, "meta": meta})
        # upsert in smaller batches to avoid memory issues
        upsert_chunks(client, to_upsert)
        to_upsert = []

    print("Upload complete.")