"""
Lecteur pour les fichiers XLS/XLSX stockes dans raw-documents (fahd_datagov.py).

Necessite le package Spark 'com.crealytics:spark-excel' (a ajouter aux jars
Spark dans spark_session.py si des ressources .xlsx sont effectivement
presentes dans les datasets data.gov.ma cibles). Fallback pandas si le
package Spark n'est pas disponible (moins scalable mais suffisant pour un MVP
ou les volumes par fichier restent petits).
"""

import logging

from pyspark.sql import DataFrame, SparkSession

from configs.spark_config import RAW_DOCUMENTS_BUCKET

logger = logging.getLogger(__name__)


def read_raw_xlsx_spark_excel(spark: SparkSession, dataset_prefix: str | None = None) -> DataFrame:
    """Lecture via le connecteur Spark natif (a privilegier si volumes importants)."""
    prefix = dataset_prefix or "source=data_gov_ma"
    path_glob = f"s3a://{RAW_DOCUMENTS_BUCKET}/{prefix}/*/*/*/*.xlsx"
    logger.info("Lecture XLSX bruts (spark-excel) : %s", path_glob)

    df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path_glob)
    )
    logger.info("Lignes XLSX lues : %s", df.count())
    return df


def read_raw_xlsx_pandas_fallback(spark: SparkSession, file_paths: list[str]) -> DataFrame:
    """
    Fallback si spark-excel n'est pas installe : lit chaque fichier via pandas
    (openpyxl) puis convertit en DataFrame Spark. A utiliser seulement pour un
    petit nombre de fichiers (pas de parallelisme distribue ici).
    """
    import pandas as pd

    from src.storage.minio.fahd_client import MinIOClient

    client = MinIOClient()
    frames = []
    for path in file_paths:
        try:
            obj = client.client.get_object(*path.split("/", 1))
            frames.append(pd.read_excel(obj.read()))
        except Exception as e:
            logger.warning("Echec lecture XLSX '%s' : %s", path, e)

    if not frames:
        raise RuntimeError("Aucun fichier XLSX n'a pu etre lu (fallback pandas).")

    pdf = pd.concat(frames, ignore_index=True)
    return spark.createDataFrame(pdf)