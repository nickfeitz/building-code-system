import os
import logging
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Embedding Service")

model = None


class EmbedRequest(BaseModel):
    text: str


class EmbedResponse(BaseModel):
    embedding: List[float]
    model: str
    dimension: int


class HealthResponse(BaseModel):
    status: str
    model: str
    model_dimension: int


@app.on_event("startup")
async def load_model():
    global model
    embedding_model = os.getenv("EMBEDDING_MODEL", "intfloat/e5-large-v2")
    logger.info(f"Loading embedding model: {embedding_model}")
    try:
        model = SentenceTransformer(embedding_model)
        logger.info(f"Model loaded successfully. Dimension: {model.get_sentence_embedding_dimension()}")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        raise


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    return {
        "status": "healthy",
        "model": os.getenv("EMBEDDING_MODEL", "intfloat/e5-large-v2"),
        "model_dimension": model.get_sentence_embedding_dimension()
    }


@app.post("/embed", response_model=EmbedResponse)
async def embed(request: EmbedRequest):
    """Generate embeddings for text"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        embedding = model.encode(request.text, convert_to_tensor=False)
        return {
            "embedding": embedding.tolist(),
            "model": os.getenv("EMBEDDING_MODEL", "intfloat/e5-large-v2"),
            "dimension": len(embedding)
        }
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.getenv("EMBEDDING_PORT", 8011))
    uvicorn.run(app, host="0.0.0.0", port=port)
