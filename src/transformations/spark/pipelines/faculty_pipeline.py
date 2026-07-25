"""
Pipeline metier faculty_profiles. Enchaine toutes les etapes et retourne un
resume utilisable pour le logging de run (jobs/faculty_profiles_job.py).
"""

import logging

from pyspark.sql import SparkSession

from configs.spark_config import RAW_LOGS_BUCKET
from src.transformations.spark.readers.json_reader import (
    read_openalex_json,
    read_web_crawler_json,
)
from src.transformations.spark.transforms.faculty_profiles_transform import (
    transform_faculty_profiles,
)
from src.transformations.spark.transforms.quality_checks import (
    deduplicate_on_record_id,
    split_valid_and_quarantine,
    write_quarantine,
)
from src.lakehouse.elasticsearch.es_writer import sync_to_elasticsearch
from src.lakehouse.hudi.hudi_writer import upsert_to_hudi
from src.lakehouse.postgres.postgres_writer import sync_to_postgres

logger = logging.getLogger(__name__)


def run_faculty_pipeline(spark: SparkSession) -> dict:
    df_web = read_web_crawler_json(spark, entity_type="faculty_profiles")
    records_read = df_web.count()

    try:
        df_openalex = read_openalex_json(spark)
    except Exception as e:
        logger.warning("Lecture OpenAlex indisponible, enrichissement ignore : %s", e)
        df_openalex = None

    df_curated = transform_faculty_profiles(df_web, df_openalex)
    df_curated.printSchema()
    df_curated.show(5, truncate=False)
    print("Curated :", df_curated.count())
    df_valid, df_quarantine = split_valid_and_quarantine(df_curated)
    quarantine_count = df_quarantine.count()
    print("Valid :", df_valid.count())
    print("Quarantine :", df_quarantine.count())
    if quarantine_count > 0:
        write_quarantine(
            df_quarantine, RAW_LOGS_BUCKET,
            "quarantine/faculty_profiles",
        )

    df_dedup, duplicates_dropped = deduplicate_on_record_id(df_valid)
    print("Dedup :", df_dedup.count())

    print("Avant Hudi :", df_dedup.count())
    df_dedup.printSchema()
    df_dedup.show(5, truncate=False)

    records_written = upsert_to_hudi(df_dedup, "faculty_profiles")

    try:
        records_synced_pg = sync_to_postgres(df_dedup, "faculty_profiles")
    except Exception as e:
        logger.error("Synchronisation Postgres (dashboard) echouee : %s", e)
        records_synced_pg = 0

    try:
        records_synced_es = sync_to_elasticsearch(df_dedup, "faculty_profiles")
    except Exception as e:
        logger.error("Indexation Elasticsearch (recherche) echouee : %s", e)
        records_synced_es = 0

    return {
        "records_read": records_read,
        "records_written": records_written,
        "records_quarantined": quarantine_count,
        "duplicates_dropped": duplicates_dropped,
        "records_synced_postgres": records_synced_pg,
        "records_synced_elasticsearch": records_synced_es,
    }