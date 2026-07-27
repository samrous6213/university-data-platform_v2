"""
university_news_etl.py
=======================

Pipeline ETL pour la table ``university_news`` du projet
University Data Platform.

Ce job Spark orchestre l'ensemble du processus ETL pour les
actualites universitaires :
    1. Lecture des enregistrements bruts depuis le bucket MinIO,
       avec explosion du tableau ``news_items``.
    2. Transformation via ``transform_university_news``.
    3. Ecriture dans la table Hudi ``university_news``.

Ce pipeline utilise ``read_raw_records`` avec le champ de tableau
``news_items`` pour exploser les listes d'actualites avant
transformation.

Ce module suit exactement la meme architecture et les memes conventions
que les autres pipelines ETL : meme style de logging, meme gestion
des erreurs.

Compatibilite : Apache Spark 3.5.1 / Apache Hudi 0.15.0

Utilisation :
    python -m src.transformations.spark.university_news_etl
"""

from __future__ import annotations

import os
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
from src.transformations.writers.hudi_writer import write_hudi_table

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# 1. Constantes
# --------------------------------------------------------------------------- #

# Champs de type tableau dans le JSON source pouvant contenir les
# enregistrements d'actualites. L'ordre definit la priorite : le
# premier champ trouve est utilise pour l'operation ``inline_outer``.
NEWS_ARRAY_FIELDS: List[str] = ["news_items"]


# --------------------------------------------------------------------------- #
# 2. Pipeline ETL principal
# --------------------------------------------------------------------------- #


def run_university_news_etl(
    spark: SparkSession,
    bucket: str = "raw-json",
) -> int:
    """
    Execute le pipeline ETL complet pour la table ``university_news``.

    Ce pipeline lit les enregistrements bruts depuis le bucket MinIO
    via ``read_raw_records`` avec explosion du tableau ``news_items``,
    applique la transformation ``transform_university_news``, puis
    ecrit le resultat dans la table Hudi ``university_news``.

    Etapes detaillees :
        1. Lecture et explosion des enregistrements bruts via
           ``read_raw_records`` avec ``array_fields=NEWS_ARRAY_FIELDS``.
        2. Filtrage si aucune donnee brute n'est trouvee.
        3. Transformation via ``transform_university_news``.
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
    logger.info("Starting university_news ETL")
    logger.info("=" * 60)

    # ----- 1. Lecture et explosion des enregistrements bruts -----

    raw = read_raw_records(spark, bucket, source_prefixes=None, array_fields=NEWS_ARRAY_FIELDS)

    if raw.count() == 0:
        logger.warning("No news data found")
        return 0

    logger.info(f"Raw news records loaded: {raw.count()}")

    # ----- 2. Transformation -----

    transformed = transform_university_news(raw)

    # ----- 3. Ecriture dans Hudi -----

    write_hudi_table(transformed, UNIVERSITY_NEWS_HUDI)

    # ----- 4. Resume -----

    final_count = transformed.count()

    logger.info(
        "university_news ETL complete",
        extra={
            "written_records": final_count,
            "table": "university_news",
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
        .appName("University News ETL")
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
    logger.info("Starting university_news ETL pipeline")
    logger.info("=" * 60)

    try:
        count = run_university_news_etl(spark, bucket=BUCKET)
        logger.info(f"ETL finished. Records written: {count}")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
