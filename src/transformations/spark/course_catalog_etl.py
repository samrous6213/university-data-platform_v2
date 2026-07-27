"""
course_catalog_etl.py
======================

Pipeline ETL pour la table ``course_catalog`` du projet
University Data Platform.

Ce job Spark orchestre l'ensemble du processus ETL pour le catalogue
de cours :
    1. Decouverte des prefixes source dans le bucket MinIO.
    2. Lecture des donnees JSON brutes depuis chaque prefixe.
    3. Explosion du tableau de cours (inline_outer).
    4. Transformation via ``transform_course_catalog``.
    5. Union de toutes les sources transformees.
    6. Ecriture dans la table Hudi ``course_catalog``.

Ce module suit exactement la meme architecture et les memes conventions
que le pipeline ``faculty_profiles`` : meme structure de code, meme
style de logging, meme gestion des erreurs.

Compatibilite : Apache Spark 3.5.1 / Apache Hudi 0.15.0

Utilisation :
    python -m src.transformations.spark.course_catalog_etl
"""

from __future__ import annotations

import os
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# 1. Constantes
# --------------------------------------------------------------------------- #

# Champs de type tableau dans le JSON source pouvant contenir les
# enregistrements de cours. L'ordre definit la priorite : le
# premier champ trouve est utilise pour l'operation ``inline_outer``.
COURSE_ARRAY_FIELDS: List[str] = [
    "course_items",
    "courses",
    "news_items",
    "items",
    "results",
    "faculty_items",
]


# --------------------------------------------------------------------------- #
# 2. Pipeline ETL principal
# --------------------------------------------------------------------------- #


def run_course_catalog_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    """
    Execute le pipeline ETL complet pour la table ``course_catalog``.

    Ce pipeline decouvre automatiquement les sources de donnees dans
    le bucket MinIO specifie, lit les fichiers JSON de chaque source,
    explose le tableau de cours, applique la transformation
    ``transform_course_catalog``, puis ecrit le resultat dans la
    table Hudi ``course_catalog``.

    Etapes detaillees :
        1. Decouverte des prefixes source via ``discover_source_prefixes``.
        2. Pour chaque prefixe :
            a. Lecture des donnees JSON via ``read_json``.
            b. Filtrage des sources vides (0 lignes).
            c. Identification du champ de type tableau (array field).
            d. Explosion du tableau via ``inline_outer``.
            e. Ajout de la colonne ``_source_prefix``.
            f. Transformation via ``transform_course_catalog``.
            g. Filtrage des sources sans donnees transformees.
        3. Union de toutes les sources transformees.
        4. Ecriture dans la table Hudi via ``write_hudi_table``.
        5. Journalisation du resume (breakdown par source + total).

    Args:
        spark: SparkSession active (creee en amont par le driver
            du job ou par Airflow).
        bucket: nom du bucket MinIO contenant les donnees brutes.
            Defaut : ``"raw-json"``.

    Returns:
        int: nombre total d'enregistrements ecrits dans la table Hudi.
            Retourne 0 si aucune donnee n'a ete trouvee ou transformee.
    """
    logger.info("=" * 60)
    logger.info("Starting course_catalog ETL")
    logger.info("=" * 60)

    transformed_sources: List[DataFrame] = []
    source_record_counts: dict = {}

    # ----- 1. Decouverte des prefixes source -----

    all_prefixes = discover_source_prefixes(spark, bucket)

    if not all_prefixes:
        logger.warning("No sources found in bucket")
        return 0

    # ----- 2. Lecture et transformation de chaque source -----

    for prefix in all_prefixes:

        raw = read_json(spark, bucket, prefix=prefix)

        if raw.count() == 0:
            continue

        matched = [f for f in COURSE_ARRAY_FIELDS if f in raw.columns]

        if not matched:
            continue

        exploded = raw.selectExpr(
            f"inline_outer({matched[0]})",
            "input_file_name() as _source_file",
        )

        source_name = extract_source_name(prefix)

        raw_count = exploded.count()

        logger.info(
            f"Source '{source_name}': {raw_count} raw records loaded ({matched[0]})"
        )

        exploded = exploded.withColumn(
            "_source_prefix",
            F.lit(source_name),
        )

        tf = transform_course_catalog(exploded)

        if tf.count() > 0:
            transformed_sources.append(tf)
            source_record_counts[source_name] = tf.count()
            logger.info(
                f"Source '{source_name}': {source_record_counts[source_name]} records after transformation"
            )

    # ----- 3. Verification des sources transformees -----

    if not transformed_sources:
        logger.warning("No course data found in any source")
        return 0

    # ----- 4. Union de toutes les sources -----

    combined = transformed_sources[0]

    for df in transformed_sources[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    # ----- 5. Ecriture dans Hudi -----

    write_hudi_table(combined, COURSE_CATALOG_HUDI)

    # ----- 6. Resume -----

    final_count = combined.count()

    breakdown = ", ".join(
        f"{src}={cnt}" for src, cnt in source_record_counts.items()
    )

    logger.info(
        f"course_catalog ETL complete | source breakdown: {breakdown} | total: {final_count} records written to {COURSE_CATALOG_HUDI.table_name}",
    )

    return final_count


# --------------------------------------------------------------------------- #
# Point d'entree
# --------------------------------------------------------------------------- #

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
BUCKET = os.getenv("MINIO_BUCKET", "raw-json")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://university-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def main():
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("Course Catalog ETL")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )

    logger.info("=" * 60)
    logger.info("Starting course_catalog ETL pipeline")
    logger.info("=" * 60)

    try:
        count = run_course_catalog_etl(spark, bucket=BUCKET)
        logger.info(f"ETL finished. Records written: {count}")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
