from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession

from src.transformations.config.hudi_config import FACULTY_PROFILES_HUDI
from src.transformations.readers.minio_reader import (
    discover_source_prefixes,
    extract_source_name,
    read_json,
)
from src.transformations.transformers.faculty_transformer import (
    transform_faculty_profiles,
)
from src.transformations.utils.logger import get_logger
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)

FACULTY_ARRAY_FIELDS: List[str] = [
    "faculty_items",
    "faculty_members",
    "results",
]

def run_faculty_profiles_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    logger.info("=" * 60)
    logger.info("Starting faculty_profiles ETL")
    logger.info("=" * 60)

    transformed_sources: List[DataFrame] = []
    source_record_counts: dict = {}

    all_prefixes = discover_source_prefixes(spark, bucket)
    if not all_prefixes:
        logger.warning("No sources found in bucket")
        return 0

    for prefix in all_prefixes:
        raw = read_json(spark, bucket, prefix=prefix)
        matched = [f for f in FACULTY_ARRAY_FIELDS if f in raw.columns]
        if not matched:
            continue
        exploded = raw.selectExpr(f"inline_outer({matched[0]})", "input_file_name() as _source_file")
        source_name = extract_source_name(prefix)
        tf = transform_faculty_profiles(exploded)
        count = tf.count()
        if count > 0:
            transformed_sources.append(tf)
            source_record_counts[source_name] = count
            logger.info(
                f"Source '{source_name}': {count} records after transformation"
            )

    if not transformed_sources:
        logger.warning("No faculty data found in any source")
        return 0

    combined = transformed_sources[0]
    for df in transformed_sources[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    write_hudi_table(combined, FACULTY_PROFILES_HUDI)

    final_count = combined.count()
    breakdown = ", ".join(
        f"{src}={cnt}" for src, cnt in source_record_counts.items()
    )
    logger.info(
        f"faculty_profiles ETL complete | source breakdown: {breakdown} | total: {final_count} records written to {FACULTY_PROFILES_HUDI.table_name}",
    )
    return final_count
