from __future__ import annotations

import os
import time
from typing import Any, Dict, Generator, Optional

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import DataFrame

from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


class ElasticsearchWriteError(Exception):
    pass


INDEX_MAPPINGS: Dict[str, Dict[str, Any]] = {
    "faculty_profiles": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "full_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "first_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "last_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "title": {"type": "text"},
            "department": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "faculty": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "university": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "email": {"type": "keyword"},
            "phone": {"type": "keyword"},
            "office": {"type": "text"},
            "profile_url": {"type": "keyword"},
            "research_interests": {"type": "text"},
            "publications_count": {"type": "integer"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
            "processing_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
        },
    },
    "course_catalog": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "course_code": {"type": "keyword"},
            "course_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "description": {"type": "text"},
            "credits": {"type": "integer"},
            "semester": {"type": "keyword"},
            "level": {"type": "keyword"},
            "department": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "faculty": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "language": {"type": "keyword"},
            "instructor": {"type": "text"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
            "processing_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
        },
    },
    "university_news": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "title": {"type": "text"},
            "summary": {"type": "text"},
            "content": {"type": "text"},
            "publication_date": {"type": "text"},
            "author": {"type": "text"},
            "faculty": {"type": "keyword"},
            "category": {"type": "keyword"},
            "language": {"type": "keyword"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
            "processing_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
        },
    },
    "research_publications": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "title": {"type": "text"},
            "abstract": {"type": "text"},
            "authors": {"type": "text"},
            "affiliations": {"type": "text"},
            "publication_year": {"type": "integer"},
            "doi": {"type": "keyword"},
            "journal": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "keywords": {"type": "text"},
            "language": {"type": "keyword"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
            "processing_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
        },
    },
    "documents_registry": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "document_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "document_type": {"type": "keyword"},
            "category": {"type": "keyword"},
            "language": {"type": "keyword"},
            "storage_path": {"type": "keyword"},
            "file_size": {"type": "keyword"},
            "checksum": {"type": "keyword"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
            "processing_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss.SSSSS||yyyy-MM-dd HH:mm:ss.SSSS||yyyy-MM-dd HH:mm:ss.SSS||strict_date_optional_time||epoch_millis"},
        },
    },
}


def _get_client(
    host: Optional[str] = None,
    port: Optional[int] = None,
    scheme: Optional[str] = None,
) -> Elasticsearch:
    host = host or os.getenv("ES_HOST")
    if host is None:
        raise ValueError(
            "ES_HOST environment variable is required. "
            "Set ES_HOST=localhost for local execution, "
            "or ES_HOST=university-elasticsearch for Docker execution."
        )
    port = port or int(os.getenv("ES_PORT", "9200"))
    scheme = scheme or os.getenv("ES_SCHEME", "http")
    return Elasticsearch([{"host": host, "port": port, "scheme": scheme}])


def _ensure_index(es: Elasticsearch, index_name: str) -> None:
    if not es.indices.exists(index=index_name):
        body: Dict[str, Any] = {}
        mapping = INDEX_MAPPINGS.get(index_name)
        if mapping:
            body["mappings"] = mapping
        es.indices.create(index=index_name, body=body)
        logger.info("Created Elasticsearch index", extra={"index": index_name})


def _generate_actions(
    df: DataFrame, index_name: str
) -> Generator[Dict[str, Any], None, None]:
    rows = df.collect()
    has_id = "record_id" in df.columns
    for row in rows:
        doc = row.asDict()
        doc = {k: v for k, v in doc.items() if v is not None}
        action: Dict[str, Any] = {"_index": index_name, "_source": doc}
        if has_id and doc.get("record_id"):
            action["_id"] = str(doc["record_id"])
        yield action


def _write_with_retry(
    df: DataFrame,
    index_name: str,
    es: Elasticsearch,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> int:
    last_error: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            success, errors = helpers.bulk(
                es,
                _generate_actions(df, index_name),
                raise_on_error=False,
                chunk_size=500,
                max_retries=2,
                request_timeout=120,
            )
            if errors:
                sample = errors[:3]
                logger.warning(
                    "Elasticsearch bulk indexing had errors",
                    extra={
                        "index": index_name,
                        "success": success,
                        "errors": len(errors),
                        "attempt": attempt,
                        "sample": sample,
                    },
                )
            else:
                logger.info(
                    "Elasticsearch bulk indexing succeeded",
                    extra={
                        "index": index_name,
                        "documents": success,
                        "attempt": attempt,
                    },
                )
            return success
        except Exception as e:
            last_error = e
            logger.warning(
                f"Elasticsearch bulk attempt {attempt}/{max_retries} failed",
                extra={"index": index_name, "error": str(e)},
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
    raise ElasticsearchWriteError(
        f"Failed to index {index_name} after {max_retries} attempts: {last_error}"
    )


def write_to_elasticsearch(
    df: DataFrame,
    index_name: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    scheme: Optional[str] = None,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> int:
    if df.count() == 0:
        logger.warning(
            "Skipping Elasticsearch write for empty DataFrame",
            extra={"index": index_name},
        )
        return 0

    es = _get_client(host, port, scheme)
    _ensure_index(es, index_name)

    logger.info(
        "Writing to Elasticsearch",
        extra={"index": index_name, "records": df.count()},
    )

    return _write_with_retry(df, index_name, es, max_retries, retry_delay)
