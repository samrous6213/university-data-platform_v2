"""
API de recherche basique (exigence brief section 3 : "1 Elasticsearch index
+ basic search endpoint").

Lancement local :
    uvicorn src.api.search_api:app --reload --port 8000

Puis :
    http://localhost:8000/docs        (doc Swagger auto)
    http://localhost:8000/search?q=intelligence+artificielle
"""

import logging

from elasticsearch import Elasticsearch
from fastapi import FastAPI, Query

from configs.spark_config import ELASTICSEARCH_INDEX, ELASTICSEARCH_URL

logger = logging.getLogger(__name__)

app = FastAPI(
    title="University Data Platform - Search API",
    description="Recherche mot-cle sur les profils faculte et le catalogue de formations.",
    version="1.0.0",
)

_client = Elasticsearch(hosts=[ELASTICSEARCH_URL], request_timeout=15)


@app.get("/health")
def health():
    """Verifie que l'API et Elasticsearch sont accessibles (ops readiness)."""
    try:
        es_ok = _client.ping()
    except Exception as e:
        logger.error("Elasticsearch injoignable : %s", e)
        es_ok = False
    return {"api": "ok", "elasticsearch": "ok" if es_ok else "unreachable"}


@app.get("/search")
def search(
    q: str = Query(..., min_length=1, description="Mot(s)-cle a rechercher"),
    size: int = Query(10, ge=1, le=50, description="Nombre de resultats max"),
):
    """
    Recherche texte libre sur les profils faculte et le catalogue de
    formations (titre, texte normalise, ecole). Retourne les meilleurs
    resultats avec un extrait surligne (highlight) du texte matche.
    """
    query = {
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["title_or_name^3", "school_name^2", "searchable_text"],
                "fuzziness": "AUTO",
            }
        },
        "highlight": {
            "fields": {"searchable_text": {"fragment_size": 150, "number_of_fragments": 1}}
        },
        "size": size,
    }

    result = _client.search(index=ELASTICSEARCH_INDEX, body=query)

    hits = []
    for hit in result["hits"]["hits"]:
        source = hit["_source"]
        highlight = hit.get("highlight", {}).get("searchable_text", [None])[0]
        hits.append({
            "score": hit["_score"],
            "entity_type": source.get("entity_type"),
            "title_or_name": source.get("title_or_name"),
            "school_name": source.get("school_name"),
            "source_url": source.get("source_url"),
            "highlight": highlight,
        })

    return {
        "query": q,
        "total_results": result["hits"]["total"]["value"],
        "results": hits,
    }