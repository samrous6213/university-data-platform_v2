"""
Writer generique pour les tables curated Hudi. Place dans src/lakehouse/ (pas dans
transformations/spark/) pour respecter la separation transform <-> lakehouse et
permettre sa reutilisation par d'autres couches futures (ex: RAG/Qdrant sync).
"""

import logging

from pyspark.sql import DataFrame

from configs.spark_config import HUDI_TABLES, MAX_WRITE_RETRIES, RETRY_BACKOFF_FACTOR
from src.transformations.spark.utils.retry import retry

from configs.spark_config import HIVE_METASTORE_URI
logger = logging.getLogger(__name__)

_HUDI_OPTIONS_BASE = {
    "hoodie.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.upsert.shuffle.parallelism": "4",
    "hoodie.insert.shuffle.parallelism": "4",
}


def _build_options(table_name: str) -> dict:
    if table_name not in HUDI_TABLES:
        raise ValueError(
            f"Table '{table_name}' non declaree dans configs.spark_config.HUDI_TABLES"
        )
    table_conf = HUDI_TABLES[table_name]
    return {
        **_HUDI_OPTIONS_BASE,
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": table_conf["recordkey_field"],
        "hoodie.datasource.write.precombine.field": table_conf["precombine_field"],
        "hoodie.datasource.write.partitionpath.field": table_conf["partitionpath_field"],
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": table_conf["partitionpath_field"],
        "hoodie.datasource.hive_sync.metastore.uris": HIVE_METASTORE_URI,
        "hoodie.datasource.hive_sync.database": "default",
    }


@retry(max_attempts=MAX_WRITE_RETRIES, backoff_factor=RETRY_BACKOFF_FACTOR, exceptions=(Exception,))
def _write_with_retry(df: DataFrame, base_path: str, options: dict) -> None:
    df.write.format("hudi").options(**options).mode("append").save(base_path)


def upsert_to_hudi(df: DataFrame, table_name: str) -> int:
    """
    Upsert idempotent vers la table Hudi `table_name`. Le nom doit exister dans
    configs.spark_config.HUDI_TABLES (faculty_profiles | course_catalog).

    Retourne le nombre de lignes ecrites.
    """
    record_count = df.count()
    if record_count == 0:
        logger.warning("Aucune ligne a ecrire pour '%s' (df vide), ecriture ignoree.", table_name)
        return 0

    table_conf = HUDI_TABLES[table_name]
    options = _build_options(table_name)

    logger.info(
        "Ecriture Hudi upsert : table=%s base_path=%s lignes=%s",
        table_name, table_conf["base_path"], record_count,
    )
    _write_with_retry(df, table_conf["base_path"], options)
    logger.info("Ecriture Hudi terminee : table=%s lignes=%s", table_name, record_count)
    return record_count