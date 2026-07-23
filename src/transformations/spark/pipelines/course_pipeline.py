"""
Pipeline metier course_catalog. Meme structure que faculty_pipeline.py,
sans etape d'enrichissement (pas de source externe equivalente a OpenAlex ici).
"""

import logging

from pyspark.sql import SparkSession

from configs.spark_config import RAW_LOGS_BUCKET
from src.transformations.spark.readers.json_reader import read_web_crawler_json
from src.transformations.spark.transforms.course_catalog_transform import (
    transform_course_catalog,
)
from src.transformations.spark.transforms.quality_checks import (
    deduplicate_on_record_id,
    split_valid_and_quarantine,
    write_quarantine,
)
from src.lakehouse.hudi_writer import upsert_to_hudi

logger = logging.getLogger(__name__)


def run_course_pipeline(spark: SparkSession) -> dict:
    df_web = read_web_crawler_json(spark, entity_type="course_catalog")
    records_read = df_web.count()

    df_curated = transform_course_catalog(df_web)

    df_valid, df_quarantine = split_valid_and_quarantine(df_curated)
    quarantine_count = df_quarantine.count()
    if quarantine_count > 0:
        write_quarantine(
            df_quarantine, RAW_LOGS_BUCKET,
            "quarantine/course_catalog",
        )

    df_dedup, duplicates_dropped = deduplicate_on_record_id(df_valid)

    records_written = upsert_to_hudi(df_dedup, "course_catalog")

    return {
        "records_read": records_read,
        "records_written": records_written,
        "records_quarantined": quarantine_count,
        "duplicates_dropped": duplicates_dropped,
    }