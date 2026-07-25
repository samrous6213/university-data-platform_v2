"""
Writer generique pour indexer les tables curated dans Elasticsearch (index
unique `university_search`), consomme par l'endpoint de recherche
(src/api/search_api.py).

Meme pattern que src/lakehouse/postgres/postgres_writer.py : client Python
pur (lib `elasticsearch`), df.collect() puis bulk indexing, aucune
dependance JVM/jar -- on evite volontairement tout ce qui a pose probleme
avec Spark JDBC.

Un seul index pour faculty_profiles ET course_catalog (champ entity_type
pour distinguer), afin de permettre une recherche unifiee sur les deux
entites depuis un seul endpoint.
"""

import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import DataFrame

from configs.spark_config import ELASTICSEARCH_INDEX, ELASTICSEARCH_URL

logger = logging.getLogger(__name__)

_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "entity_type": {"type": "keyword"},
            "record_id": {"type": "keyword"},
            "school_id": {"type": "keyword"},
            "school_name": {
                "type": "text",
                "fields": {"raw": {"type": "keyword"}},
            },
            "title_or_name": {"type": "text"},
            "searchable_text": {"type": "text"},
            "source_url": {"type": "keyword"},
            "crawl_timestamp": {"type": "date"},
        }
    }
}

_index_ensured = False


def _get_client() -> Elasticsearch:
    return Elasticsearch(hosts=[ELASTICSEARCH_URL], request_timeout=15)


def _ensure_index(client: Elasticsearch) -> None:
    global _index_ensured
    if _index_ensured:
        return
    if not client.indices.exists(index=ELASTICSEARCH_INDEX):
        client.indices.create(index=ELASTICSEARCH_INDEX, body=_INDEX_MAPPING)
        logger.info("Index Elasticsearch '%s' cree.", ELASTICSEARCH_INDEX)
    _index_ensured = True


def _join_non_null(*parts) -> str:
    return " ".join(str(p) for p in parts if p not in (None, ""))


def _build_doc(row, table_name: str) -> dict:
    if table_name == "faculty_profiles":
        title_or_name = row["full_name"] or row["title"] or row["school_name"]
        searchable_text = _join_non_null(
            row["full_name"], row["title"], row["department"], row["email"],
            row["school_name"], row["normalized_text"],
        )
    else:  # course_catalog
        title_or_name = row["program_name"] or row["school_name"]
        searchable_text = _join_non_null(
            row["program_name"], row["program_level"], row["department"],
            row["school_name"], row["normalized_text"],
        )

    return {
        "entity_type": table_name,
        "record_id": row["record_id"],
        "school_id": row["school_id"],
        "school_name": row["school_name"],
        "title_or_name": title_or_name,
        "searchable_text": searchable_text,
        "source_url": row["source_url"],
        "crawl_timestamp": row["crawl_timestamp"].isoformat() if row["crawl_timestamp"] else None,
    }


def sync_to_elasticsearch(df: DataFrame, table_name: str) -> int:
    """
    Indexe (upsert, _id=record_id) les lignes de `df` dans l'index unifie
    `university_search`. A appeler juste apres sync_to_postgres(), avec le
    meme DataFrame deja deduplique/valide. Idempotent grace a _id=record_id
    (un rerun reindexe les memes documents plutot que de dupliquer).
    """
    record_count = df.count()
    if record_count == 0:
        logger.warning("Aucune ligne a indexer dans Elasticsearch pour '%s'.", table_name)
        return 0

    client = _get_client()
    _ensure_index(client)

    rows = df.collect()  # OK pour un MVP (tables curated de taille modeste)

    def _actions():
        for row in rows:
            doc = _build_doc(row, table_name)
            yield {
                "_index": ELASTICSEARCH_INDEX,
                "_id": doc["record_id"],
                "_source": doc,
            }

    success_count, errors = bulk(client, _actions(), raise_on_error=False, stats_only=False)
    if errors:
        logger.warning(
            "Indexation Elasticsearch '%s' : %s succes, %s erreurs (voir details ci-dessous).",
            table_name, success_count, len(errors),
        )
        for err in errors[:5]:
            logger.warning("Erreur bulk ES : %s", err)
    else:
        logger.info("Indexation Elasticsearch terminee : table=%s documents=%s", table_name, success_count)

    return success_count