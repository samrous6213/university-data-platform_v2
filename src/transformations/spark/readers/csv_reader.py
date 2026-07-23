"""
Lecteur pour les fichiers CSV stockes dans raw-documents (fahd_datagov.py).

Utilise l'inference de schema Spark native (pas de logique custom), avec
gestion des encodages courants sur les portails data.gov marocains (souvent
latin-1/cp1252 plutot que utf-8 strict).
"""

import logging

from pyspark.sql import DataFrame, SparkSession

from configs.spark_config import RAW_DOCUMENTS_BUCKET

logger = logging.getLogger(__name__)


def read_raw_csv(
    spark: SparkSession,
    dataset_prefix: str | None = None,
    encoding: str = "UTF-8",
    delimiter: str = ",",
) -> DataFrame:
    """
    Lit les CSV sous raw-documents. Si l'encodage UTF-8 echoue silencieusement
    (caracteres accentues corrompus), reessayer avec encoding='ISO-8859-1'.
    """
    prefix = dataset_prefix or "source=data_gov_ma"
    path_glob = f"s3a://{RAW_DOCUMENTS_BUCKET}/{prefix}/*/*/*/*.csv"
    logger.info("Lecture CSV bruts : %s (encoding=%s)", path_glob, encoding)

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", encoding)
        .option("delimiter", delimiter)
        .option("mode", "PERMISSIVE")
        .csv(path_glob)
    )

    logger.info("Lignes CSV lues : %s", df.count())
    return df