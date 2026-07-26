from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from src.transformations.config.hudi_config import COURSE_CATALOG_HUDI
from src.transformations.readers.minio_reader import (
    discover_source_prefixes,
    extract_source_name,
    read_json,
)
from src.transformations.transformers.course_transformer import (
    transform_course_catalog,
)
from src.transformations.utils.logger import get_logger
from src.transformations.writers.es_writer import write_to_elasticsearch
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)

COURSE_ARRAY_FIELDS: List[str] = [
    "course_items",
    "courses",
    "news_items",
    "items",
    "results",
    "faculty_items",
]


def run_course_catalog_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    logger.info("=" * 60)
    logger.info("Starting course_catalog ETL")
    logger.info("=" * 60)

    transformed_sources: List[DataFrame] = []
    source_record_counts: dict = {}

    all_prefixes = discover_source_prefixes(spark, bucket)
    if not all_prefixes:
        logger.warning("No sources found in bucket")
        return 0

    for prefix in all_prefixes:
        raw = read_json(spark, bucket, prefix=prefix)
        if len(raw.columns) == 1 and "_corrupt_record" in raw.columns:
            raw = read_json(spark, bucket, prefix=prefix, multi_line=True)
        matched = [f for f in COURSE_ARRAY_FIELDS if f in raw.columns]
        source_name = extract_source_name(prefix)
        if matched:
            exploded = raw.selectExpr(f"inline_outer({matched[0]})", "input_file_name() as _source_file")
        else:
            logger.info(f"No array field matched for source='{source_name}', treating as flat record")
            exploded = raw.withColumn("_source_file", F.input_file_name())
        tf = transform_course_catalog(exploded, source_name)
        count = tf.count()
        if count > 0:
            transformed_sources.append(tf)
            source_record_counts[source_name] = count
            logger.info(
                f"Source '{source_name}': {count} records after transformation"
            )

    if not transformed_sources:
        logger.warning("No course data found in any source")
        return 0

    combined = transformed_sources[0]
    for df in transformed_sources[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    written_count =     write_hudi_table(combined, COURSE_CATALOG_HUDI)

    write_to_elasticsearch(combined.dropDuplicates(["record_id"]), "course_catalog")

    raw_total = sum(source_record_counts.values())
    breakdown = ", ".join(
        f"{src}={cnt}" for src, cnt in source_record_counts.items()
    )
    logger.info(
        f"course_catalog ETL complete | source breakdown: {breakdown} | raw_total: {raw_total} | unique_written: {written_count} records written to {COURSE_CATALOG_HUDI.table_name}",
    )
    return written_count
