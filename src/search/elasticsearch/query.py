r"""
Étape 8 (bis) : Requêtes de recherche sur les index Elasticsearch créés par index.py.

Fournit :
  - des fonctions de recherche réutilisables (full-text, filtres, facettes)
    sur les 3 index : faculty_profiles, research_publications, university_news
  - une API HTTP Flask exposant ces fonctions comme endpoints (exigence du
    cahier des charges : "basic search endpoint")

Emplacement prévu : src/search/elasticsearch/query.py

Usage :
  # Mode CLI (test rapide en ligne de commande) :
  docker exec spark-master python3 /workspace/src/search/elasticsearch/query.py "intelligence artificielle"

  # Mode API HTTP (serveur Flask, écoute sur le port 5000) :
  docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
"""

import os
import sys
import json
from elasticsearch import Elasticsearch
from flask import Flask, request, jsonify

ES_HOST = os.getenv("ES_HOST", "http://university-elasticsearch:9200")

INDICES = ["faculty_profiles", "research_publications", "university_news"]


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST)


def search_keyword(es: Elasticsearch, query: str, index: str = None, size: int = 10):
    """
    Recherche full-text simple sur un ou tous les index.
    Utilise 'multi_match' pour chercher le mot-clé sur tous les champs texte.
    """
    target_index = index if index else ",".join(INDICES)

    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["*"],
                "fuzziness": "AUTO",
            }
        },
        "size": size,
    }

    result = es.search(index=target_index, body=body)
    return result["hits"]["hits"]


def search_with_filter(es: Elasticsearch, index: str, query: str, filters: dict, size: int = 10):
    """
    Recherche full-text combinée à des filtres exacts (ex: category='general',
    department='Informatique').
    """
    must_clauses = [{"multi_match": {"query": query, "fields": ["*"]}}] if query else []
    filter_clauses = [{"term": {f"{k}.keyword": v}} for k, v in filters.items()]

    body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses,
            }
        },
        "size": size,
    }

    result = es.search(index=index, body=body)
    return result["hits"]["hits"]


def facet_count(es: Elasticsearch, index: str, field: str, size: int = 20):
    """
    Retourne le nombre de documents par valeur distincte d'un champ
    (ex: nombre de news par category, nombre de profs par department).
    """
    body = {
        "size": 0,
        "aggs": {
            "facet": {
                "terms": {"field": f"{field}.keyword", "size": size}
            }
        },
    }

    result = es.search(index=index, body=body)
    buckets = result["aggregations"]["facet"]["buckets"]
    return [(b["key"], b["doc_count"]) for b in buckets]


def print_hits(hits):
    if not hits:
        print("Aucun résultat.")
        return
    for hit in hits:
        print(f"[{hit['_index']}] score={hit['_score']:.2f} id={hit['_id']}")
        print(json.dumps(hit["_source"], ensure_ascii=False, indent=2)[:500])
        print("-" * 60)


# ==============================================================
# API HTTP (Flask) — exigence "basic search endpoint" du cahier des charges
# ==============================================================

app = Flask(__name__)


def _serialize_hits(hits):
    return [
        {"index": h["_index"], "id": h["_id"], "score": h["_score"], "source": h["_source"]}
        for h in hits
    ]


@app.route("/health", methods=["GET"])
def health_endpoint():
    es = get_es_client()
    try:
        info = es.info()
        return jsonify({"status": "ok", "cluster": info.get("cluster_name")}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 503


@app.route("/search", methods=["GET"])
def search_endpoint():
    es = get_es_client()
    query = request.args.get("q", "").strip()
    index = request.args.get("index")
    size = int(request.args.get("size", 10))

    if not query:
        return jsonify({"error": "Le paramètre 'q' est requis."}), 400
    if index and index not in INDICES:
        return jsonify({"error": f"'index' doit être l'un de : {INDICES}"}), 400

    hits = search_keyword(es, query, index=index, size=size)
    return jsonify({
        "query": query,
        "index": index or "all",
        "total": len(hits),
        "results": _serialize_hits(hits),
    }), 200


@app.route("/search/filter", methods=["GET"])
def filter_endpoint():
    es = get_es_client()
    index = request.args.get("index", "").strip()
    query = request.args.get("q", "").strip()
    field = request.args.get("field", "").strip()
    value = request.args.get("value", "").strip()
    size = int(request.args.get("size", 10))

    if index not in INDICES:
        return jsonify({"error": f"'index' doit être l'un de : {INDICES}"}), 400
    if not field or not value:
        return jsonify({"error": "'field' et 'value' sont requis."}), 400

    hits = search_with_filter(es, index, query, {field: value}, size=size)
    return jsonify({
        "index": index,
        "filter": {field: value},
        "total": len(hits),
        "results": _serialize_hits(hits),
    }), 200


@app.route("/facets", methods=["GET"])
def facets_endpoint():
    es = get_es_client()
    index = request.args.get("index", "").strip()
    field = request.args.get("field", "").strip()

    if index not in INDICES:
        return jsonify({"error": f"'index' doit être l'un de : {INDICES}"}), 400
    if not field:
        return jsonify({"error": "'field' est requis."}), 400

    buckets = facet_count(es, index, field)
    return jsonify({
        "index": index,
        "field": field,
        "facets": [{"value": k, "count": c} for k, c in buckets],
    }), 200


if __name__ == "__main__":
    # Mode API HTTP : python3 query.py --serve
    if "--serve" in sys.argv:
        app.run(host="0.0.0.0", port=5000, debug=False)

    # Mode CLI historique : python3 query.py "mot-clé"
    elif len(sys.argv) > 1:
        es = get_es_client()
        query = " ".join(sys.argv[1:])
        print(f"Recherche full-text pour : '{query}'\n")
        hits = search_keyword(es, query)
        print_hits(hits)

    # Aucun argument : exemple par défaut (facette de démonstration)
    else:
        es = get_es_client()
        print("Exemple de facette : news par catégorie")
        print(facet_count(es, "university_news", "category"))