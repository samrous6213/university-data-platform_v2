from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from src.transformations.config.hudi_config import RESEARCH_PUBLICATIONS_HUDI
from src.transformations.readers.minio_reader import read_raw_records
from src.transformations.transformers.publications_transformer import (
    transform_research_publications,
)
from src.transformations.utils.logger import get_logger
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)

PUB_ARRAY_FIELDS: List[str] = ["results"]


def run_research_publications_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    logger.info("=" * 60)
    logger.info("Starting research_publications ETL")
    logger.info("=" * 60)

    raw = read_raw_records(spark, bucket, source_prefixes=None, array_fields=PUB_ARRAY_FIELDS)

    if raw.count() == 0:
        logger.warning("No publication data found")
        return 0

    logger.info(f"Raw publication records loaded: {raw.count()}")

    transformed = transform_research_publications(raw)
    write_hudi_table(transformed, RESEARCH_PUBLICATIONS_HUDI)

    final_count = transformed.count()
    logger.info("research_publications ETL complete", extra={"written_records": final_count, "table": "research_publications"})
    return final_count
