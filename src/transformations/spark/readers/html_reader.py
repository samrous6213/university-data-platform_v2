"""
Lecteur pour le bucket raw-web-html.

Utilise uniquement en fallback : generic_crawler.py stocke deja le texte extrait
(BeautifulSoup) dans le JSON jumeau (raw-json), donc read_web_crawler_json()
suffit dans la majorite des cas. Ce reader sert quand extracted_text est vide/trop
court (page fortement JS, extraction ratee) et qu'on doit retenter le parsing HTML
brut directement dans Spark.
"""

import logging

from bs4 import BeautifulSoup
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from configs.spark_config import RAW_WEB_HTML_BUCKET

logger = logging.getLogger(__name__)


@udf(returnType=StringType())
def _extract_text_udf(html_bytes: bytes) -> str:
    if not html_bytes:
        return ""
    try:
        soup = BeautifulSoup(html_bytes, "html.parser")
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return ""


def read_raw_html(spark: SparkSession, entity_type: str) -> DataFrame:
    """
    Lit les fichiers HTML bruts en binaire (format Spark 'binaryFile') et extrait
    le texte via BeautifulSoup en UDF. Retourne (path, content, extracted_text).
    """
    path_glob = f"s3a://{RAW_WEB_HTML_BUCKET}/source=*/entity={entity_type}/*/*/*/*.html"
    logger.info("Lecture HTML brut (fallback) : %s", path_glob)

    df = spark.read.format("binaryFile").load(path_glob)
    df = df.withColumn("extracted_text", _extract_text_udf(df["content"]))
    df = df.drop("content")  # ne pas garder les octets bruts en memoire au-dela du parsing

    logger.info("Pages HTML relues en fallback : %s", df.count())
    return df