from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.transformations.config.hudi_config import UNIVERSITY_NEWS_HUDI
from src.transformations.readers.minio_reader import read_raw_records
from src.transformations.transformers.news_transformer import (
    transform_university_news,
)
from src.transformations.utils.logger import get_logger
from src.transformations.writers.es_writer import write_to_elasticsearch
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)

NEWS_ARRAY_FIELDS: List[str] = ["news_items"]


def run_university_news_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    logger.info("=" * 60)
    logger.info("Starting university_news ETL")
    logger.info("=" * 60)

    raw = read_raw_records(spark, bucket, source_prefixes=None, array_fields=NEWS_ARRAY_FIELDS)

    if raw.count() == 0:
        logger.warning("No news data found")
        return 0

    logger.info(f"Raw news records loaded: {raw.count()}")

    transformed = transform_university_news(raw)
    write_hudi_table(transformed, UNIVERSITY_NEWS_HUDI)
    write_to_elasticsearch(transformed, "university_news")

    final_count = transformed.count()
    logger.info("university_news ETL complete", extra={"written_records": final_count, "table": "university_news"})
    return final_count
