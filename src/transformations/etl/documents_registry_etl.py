from __future__ import annotations

from pyspark.sql import SparkSession

from src.transformations.config.hudi_config import DOCUMENTS_REGISTRY_HUDI
from src.transformations.readers.minio_reader import read_raw_records
from src.transformations.transformers.documents_transformer import (
    transform_documents_registry,
)
from src.transformations.utils.logger import get_logger
from src.transformations.writers.es_writer import write_to_elasticsearch
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)

def run_documents_registry_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    logger.info("=" * 60)
    logger.info("Starting documents_registry ETL")
    logger.info("=" * 60)

    raw = read_raw_records(spark, bucket, source_prefixes=None, array_fields=[])
    if raw.count() == 0:
        logger.warning("No document data found")
        return 0

    logger.info(f"Raw document records loaded: {raw.count()}")

    transformed = transform_documents_registry(raw)
    write_hudi_table(transformed, DOCUMENTS_REGISTRY_HUDI)
    write_to_elasticsearch(transformed, "documents_registry")

    final_count = transformed.count()
    logger.info("documents_registry ETL complete", extra={"written_records": final_count, "table": "documents_registry"})
    return final_count
