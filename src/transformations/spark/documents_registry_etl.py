"""
documents_registry_etl.py
==========================

Pipeline ETL pour la table ``documents_registry`` du projet
University Data Platform.

Ce job Spark orchestre l'ensemble du processus ETL pour le registre
de documents :
    1. Lecture des enregistrements bruts depuis le bucket MinIO.
    2. Transformation via ``transform_documents_registry``.
    3. Ecriture dans la table Hudi ``documents_registry``.

Contrairement aux pipelines ``course_catalog`` et ``faculty_profiles``
qui utilisent ``read_json`` + ``inline_outer`` pour exploser des
tableaux, ce pipeline lit les enregistrements bruts via
``read_raw_records`` (JSON deja aplati).

Ce module suit exactement la meme architecture et les memes conventions
que les autres pipelines ETL : meme style de logging, meme gestion
des erreurs.

Compatibilite : Apache Spark 3.5.1 / Apache Hudi 0.15.0

Utilisation :
    python -m src.transformations.spark.documents_registry_etl
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.transformations.config.hudi_config import DOCUMENTS_REGISTRY_HUDI
from src.transformations.readers.minio_reader import read_raw_records
from src.transformations.transformers.documents_transformer import (
    transform_documents_registry,
)
from src.transformations.utils.logger import get_logger
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Pipeline ETL principal
# --------------------------------------------------------------------------- #


def run_documents_registry_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    """
    Execute le pipeline ETL complet pour la table ``documents_registry``.

    Ce pipeline lit les enregistrements bruts depuis le bucket MinIO
    via ``read_raw_records`` (sans explosion de tableau), applique la
    transformation ``transform_documents_registry``, puis ecrit le
    resultat dans la table Hudi ``documents_registry``.

    Etapes detaillees :
        1. Lecture des enregistrements bruts via ``read_raw_records``.
        2. Filtrage si aucune donnee brute n'est trouvee.
        3. Transformation via ``transform_documents_registry``.
        4. Ecriture dans la table Hudi via ``write_hudi_table``.
        5. Journalisation du resume.

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
    logger.info("Starting documents_registry ETL")
    logger.info("=" * 60)

    # ----- 1. Lecture des enregistrements bruts -----

    raw = read_raw_records(spark, bucket, source_prefixes=None, array_fields=[])

    if raw.count() == 0:
        logger.warning("No document data found")
        return 0

    logger.info(f"Raw document records loaded: {raw.count()}")

    # ----- 2. Transformation -----

    transformed = transform_documents_registry(raw)

    # ----- 3. Ecriture dans Hudi -----

    write_hudi_table(transformed, DOCUMENTS_REGISTRY_HUDI)

    # ----- 4. Resume -----

    final_count = transformed.count()

    logger.info(
        "documents_registry ETL complete",
        extra={
            "written_records": final_count,
            "table": "documents_registry",
        },
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
        .appName("Documents Registry ETL")
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
    logger.info("Starting documents_registry ETL pipeline")
    logger.info("=" * 60)

    try:
        count = run_documents_registry_etl(spark, bucket=BUCKET)
        logger.info(f"ETL finished. Records written: {count}")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
